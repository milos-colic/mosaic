package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOLYGON, POLYGON}
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class JTSMultiPolygon(multiPolygon: MultiPolygon) extends JTSGeometry(multiPolygon) {

    override def toInternal: InternalGeometry = {
        val n = multiPolygon.getNumGeometries
        val polygons = for (i <- 0 until n) yield JTSPolygon(multiPolygon.getGeometryN(i)).toInternal
        val boundaries = polygons.map(_.boundaries.head).toArray
        val holes = polygons.flatMap(_.holes).toArray
        new InternalGeometry(MULTIPOLYGON.id, getSpatialReference, boundaries, holes)
    }

    override def getBoundary: JTSGeometry = {
        val geom = multiPolygon.getBoundary
        geom.setSRID(multiPolygon.getSRID)
        JTSGeometry(geom)
    }

    override def getShells: Seq[JTSLineString] = {
        val n = multiPolygon.getNumGeometries
        val shells = for (i <- 0 until n) yield {
            val polygon = JTSPolygon(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
            polygon.getShells
        }
        shells.flatten
    }

    override def getHoles: Seq[Seq[JTSLineString]] = {
        val n = multiPolygon.getNumGeometries
        val holes = for (i <- 0 until n) yield {
            val polygon = JTSPolygon(multiPolygon.getGeometryN(i).asInstanceOf[Polygon])
            polygon.getHoles
        }
        holes.flatten
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry = {
        JTSMultiPolygon.fromSeq(
          asSeq.map(_.asInstanceOf[JTSPolygon].mapXY(f).asInstanceOf[JTSPolygon])
        )
    }

    override def asSeq: Seq[JTSGeometry] =
        for (i <- 0 until multiPolygon.getNumGeometries) yield {
            val geom = multiPolygon.getGeometryN(i)
            geom.setSRID(multiPolygon.getSRID)
            JTSGeometry(geom)
        }

    override def flatten: Seq[JTSGeometry] = asSeq

    override def getShellPoints: Seq[Seq[JTSPoint]] = getShells.map(_.asSeq)

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = getHoles.map(_.map(_.asSeq))

}

object JTSMultiPolygon extends GeometryReader {

    override def fromInternal(row: InternalRow): JTSGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)

        gf.createLinearRing(gf.createLineString().getCoordinates)
        val polygons = internalGeom.boundaries.zip(internalGeom.holes).map { case (boundaryRing, holesRings) =>
            val shell = gf.createLinearRing(boundaryRing.map(_.toCoordinate))
            val holes = holesRings.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
            gf.createPolygon(shell, holes)
        }
        val multiPolygon = gf.createMultiPolygon(polygons)
        multiPolygon.setSRID(internalGeom.srid)
        JTSMultiPolygon(multiPolygon)
    }

    override def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOLYGON): JTSMultiPolygon = {

        val gf = new GeometryFactory()

        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSMultiPolygon(gf.createMultiPolygon())
        }

        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POLYGON                       =>
                val extractedPolys = geomSeq.map(_.asInstanceOf[JTSPolygon])
                gf.createMultiPolygon(extractedPolys.map(_.getGeom.asInstanceOf[Polygon]).toArray)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSMultiPolygon(newGeom)
    }

    def apply(multiPolygon: Geometry): JTSMultiPolygon = new JTSMultiPolygon(multiPolygon.asInstanceOf[MultiPolygon])

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
