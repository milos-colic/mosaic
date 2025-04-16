package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

import java.util

class JTSGeometryCollection(geomCollection: GeometryCollection)
    extends JTSGeometry(geomCollection) {

    override def toInternal: InternalGeometry = {
        val n = geomCollection.getNumGeometries
        val geoms = for (i <- 0 until n) yield JTSGeometry(geomCollection.getGeometryN(i))
        val flattened = geoms
            .flatMap { g =>
                // By convention internal representation forces flattening of MULTI geometries
                GeometryTypeEnum.fromString(g.getGeometryType) match {
                    case MULTIPOLYGON    => g.flatten
                    case MULTILINESTRING => g.flatten
                    case MULTIPOINT      => g.flatten
                    case _               => Seq(g)
                }
            }
            .map {
                case p: JTSPolygon    => p.toInternal
                case l: JTSLineString =>
                    val shell = l.toInternal.boundaries.head
                    // By convention linestrings are polygons with no shells and one hole
                    new InternalGeometry(POLYGON.id, l.getSpatialReference, Array(Array.empty), Array(Array(shell)))
                case p: JTSPoint      =>
                    val shell = p.toInternal.boundaries.head
                    // By convention points are polygons with no shells and one hole with one point
                    new InternalGeometry(POLYGON.id, p.getSpatialReference, Array(Array.empty), Array(Array(shell)))
            }
        val boundaries = flattened.map(_.boundaries.head).toArray
        val holes = flattened.flatMap(_.holes).toArray
        new InternalGeometry(GEOMETRYCOLLECTION.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: JTSGeometry = boundary

    override def getShells: Seq[JTSLineString] = {
        val n = geomCollection.getNumGeometries
        val shells = for (i <- 0 until n) yield {
            GeometryTypeEnum.fromString(geomCollection.getGeometryN(i).getGeometryType) match {
                case GEOMETRYCOLLECTION =>
                    JTSGeometryCollection(geomCollection.getGeometryN(i).asInstanceOf[GeometryCollection]).getShells
                case MULTIPOLYGON       => JTSMultiPolygon(geomCollection.getGeometryN(i).asInstanceOf[MultiPolygon]).getShells
                case POLYGON            => JTSPolygon(geomCollection.getGeometryN(i).asInstanceOf[Polygon]).getShells
                case _                  => Seq.empty
            }
        }
        shells.flatten
    }

    override def getHoles: Seq[Seq[JTSLineString]] = {
        val n = geomCollection.getNumGeometries
        val holes = for (i <- 0 until n) yield {
            GeometryTypeEnum.fromString(geomCollection.getGeometryN(i).getGeometryType) match {
                case GEOMETRYCOLLECTION =>
                    JTSGeometryCollection(geomCollection.getGeometryN(i).asInstanceOf[GeometryCollection]).getHoles
                case MULTIPOLYGON       => JTSMultiPolygon(geomCollection.getGeometryN(i).asInstanceOf[MultiPolygon]).getHoles
                case POLYGON            => JTSPolygon(geomCollection.getGeometryN(i).asInstanceOf[Polygon]).getHoles
                case _                  => Seq.empty
            }
        }
        holes.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry = {
        JTSGeometryCollection
            .fromSeq(
              asSeq.map(_.mapXY(f))
            )
            .asInstanceOf[JTSGeometry]
    }

    override def asSeq: Seq[JTSGeometry] =
        for (i <- 0 until geomCollection.getNumGeometries) yield {
            val geom = geomCollection.getGeometryN(i)
            geom.setSRID(geomCollection.getSRID)
            JTSGeometry(geom)
        }

    override def flatten: Seq[JTSGeometry] = asSeq

    override def getShellPoints: Seq[Seq[JTSPoint]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = getHoles.map(_.map(_.asSeq))

    override def union(other: JTSGeometry): JTSGeometry = {
        val union = this.collectionUnion(other)
        union.setSRID(this.getSpatialReference)
        JTSGeometry(union)
    }

    override def difference(other: JTSGeometry): JTSGeometry = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = other.flatten.map(_.getGeom)
        val differences = leftGeoms.map(geom =>
            if (rightGeoms.length == 1) geom.difference(rightGeoms.head)
            else rightGeoms.map(geom.difference)
                .foldLeft(geom)((a, b) => a.intersection(b))
        )
        JTSGeometry(JTSGeometry.compactCollection(differences, getSpatialReference))
    }

    override def boundary: JTSGeometry = {
        val boundary = this.flatten.map(_.boundary.getGeom)
        JTSGeometry(JTSGeometry.compactCollection(boundary, getSpatialReference))
    }

    private def collectionUnion(other: JTSGeometry): Geometry = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = other.flatten.map(_.getGeom)
        JTSGeometry.compactCollection(leftGeoms ++ rightGeoms, getSpatialReference)
    }

    override def distance(geom2: JTSGeometry): Double = {
        val leftGeoms = this.flatten.map(_.getGeom)
        val rightGeoms = geom2.flatten.map(_.getGeom)
        leftGeoms.map(geom => rightGeoms.map(geom.distance).min).min
    }
}

object JTSGeometryCollection extends GeometryReader {

    override def fromInternal(row: InternalRow): JTSGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        val rings = internalGeom.boundaries.zip(internalGeom.holes)

        val multipolygon = rings
            .filter(_._1.nonEmpty)
            .map { case (boundaryRing, holesRings) => JTSPolygon.fromRings(boundaryRing, holesRings, internalGeom.srid) }
            .reduceOption(_ union _)

        val multilinestring = rings
            .filter(r => r._1.isEmpty && r._2.nonEmpty && r._2.head.length > 1)
            .map { case (_, holesRings) =>
                val linestring = JTSLineString.fromSeq(holesRings.head.map(ic => {
                    val point = JTSPoint.apply(ic.coords)
                    point.setSpatialReference(internalGeom.srid)
                    point
                }))
                linestring.setSpatialReference(internalGeom.srid)
                linestring.asInstanceOf[JTSGeometry]
            }
            .reduceOption(_ union _)

        val multipoint = rings
            .filter(r => r._1.isEmpty && r._2.nonEmpty && r._2.head.length == 1)
            .map { case (_, holesRings) =>
                val point = JTSPoint.apply(holesRings.head.head.coords)
                point.setSpatialReference(internalGeom.srid)
                point.asInstanceOf[JTSGeometry]
            }
            .reduceOption(_ union _)

        val geometries = new util.ArrayList[Geometry]()
        multipoint.map(g => geometries.add(g.getGeom))
        multilinestring.map(g => geometries.add(g.getGeom))
        multipolygon.map(g => geometries.add(g.getGeom))

        val geometry = gf.buildGeometry(geometries)

        JTSGeometryCollection(geometry)
    }

    override def fromSeq[T <: JTSGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = GEOMETRYCOLLECTION
    ): JTSGeometry = {
        JTSGeometryCollection(
          geomSeq
              .map(_.asInstanceOf[JTSGeometry])
              .reduce((a, b) => a.union(b))
              .getGeom
        ).asInstanceOf[JTSGeometry]

    }

    def apply(geomCollection: Geometry): JTSGeometryCollection =
        new JTSGeometryCollection(geomCollection.asInstanceOf[GeometryCollection])

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
