package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types.model._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class JTSPolygon(polygon: Polygon) extends JTSGeometry(polygon) {

    override def toInternal: InternalGeometry = {
        val boundary = polygon.getBoundary
        val shell = boundary.getGeometryN(0).getCoordinates.map(InternalCoord(_))
        val holes = for (i <- 1 until boundary.getNumGeometries) yield boundary.getGeometryN(i).getCoordinates.map(InternalCoord(_))
        new InternalGeometry(POLYGON.id, getSpatialReference, Array(shell), Array(holes.toArray))
    }

    override def getBoundary: JTSGeometry = {
        val boundaryRing = polygon.getBoundary
        boundaryRing.setSRID(polygon.getSRID)
        JTSGeometry(boundaryRing)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry = {
        val shellTransformed = getShells.head.mapXY(f)
        val holesTransformed = getHoles.head.map(_.mapXY(f))
        val newGeom = JTSPolygon.fromSeq(Seq(shellTransformed) ++ holesTransformed)
        newGeom.setSpatialReference(getSpatialReference)
        newGeom
    }

    override def getShells: Seq[JTSLineString] = {
        val ring = polygon.getExteriorRing
        ring.setSRID(polygon.getSRID)
        Seq(JTSLineString(ring))
    }

    override def getHoles: Seq[Seq[JTSLineString]] =
        Seq(for (i <- 0 until polygon.getNumInteriorRing) yield {
            val ring = polygon.getInteriorRingN(i)
            ring.setSRID(polygon.getSRID)
            JTSLineString(ring)
        })

    override def asSeq: Seq[JTSLineString] = getShells ++ getHoles.flatten

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = getHoles.map(_.map(_.asSeq))

    override def flatten: Seq[JTSGeometry] = List(this)

    override def getShellPoints: Seq[Seq[JTSPoint]] = getShells.map(_.asSeq)

}

object JTSPolygon extends GeometryReader {

    def fromRings(boundaryRing: Array[InternalCoord], holesRings: Array[Array[InternalCoord]], srid: Int): JTSGeometry = {
        val gf = new GeometryFactory()
        val shell = gf.createLinearRing(boundaryRing.map(_.toCoordinate))
        val holes = holesRings.map(ring => ring.map(_.toCoordinate)).map(gf.createLinearRing)
        val geometry = gf.createPolygon(shell, holes)
        geometry.setSRID(srid)
        JTSGeometry(geometry)
    }

    def getPoints(linearRing: LinearRing): Seq[JTSPoint] = {
        linearRing.getCoordinates.map(JTSPoint(_, linearRing.getSRID))
    }

    override def fromInternal(row: InternalRow): JTSGeometry = {
        val internalGeom = InternalGeometry(row)
        fromRings(internalGeom.boundaries.head, internalGeom.holes.head, internalGeom.srid)
    }

    override def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POLYGON): JTSPolygon = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSPolygon(gf.createPolygon())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[JTSPoint])
                val exteriorRing =
                    if (extractedPoints.head.coord == extractedPoints.last.coord) {
                        extractedPoints.map(_.coord).toArray
                    } else {
                        extractedPoints.map(_.coord).toArray ++ Array(extractedPoints.head.coord)
                    }
                gf.createPolygon(exteriorRing)
            case LINESTRING                    =>
                val extractedLines = geomSeq.map(_.asInstanceOf[JTSLineString])
                val exteriorRing =
                    if (extractedLines.head.asSeq.head.coord == extractedLines.head.asSeq.last.coord) {
                        gf.createLinearRing(extractedLines.head.asSeq.map(_.coord).toArray)
                    } else {
                        gf.createLinearRing(extractedLines.head.asSeq.map(_.coord).toArray ++ Array(extractedLines.head.asSeq.head.coord))
                    }
                val holes = extractedLines.tail
                    .map({ h: JTSLineString =>
                        if (h.asSeq.head.coord == h.asSeq.last.coord) {
                            h.asSeq.map(_.coord).toArray
                        } else {
                            h.asSeq.map(_.coord).toArray ++ Array(h.asSeq.head.coord)
                        }
                    })
                    .map(gf.createLinearRing)
                    .toArray
                gf.createPolygon(exteriorRing, holes)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSPolygon(newGeom)
    }

    def apply(geometry: Geometry): JTSPolygon = {
        new JTSPolygon(geometry.asInstanceOf[Polygon])
    }

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
