package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types.model._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class JTSLineString(lineString: LineString) extends JTSGeometry(lineString) {

    override def getShellPoints: Seq[Seq[JTSPoint]] = Seq(JTSLineString.getPoints(lineString))

    override def toInternal: InternalGeometry = {
        val shell = lineString.getCoordinates.map(InternalCoord(_))
        new InternalGeometry(LINESTRING.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def getBoundary: JTSGeometry = {
        val geom = lineString.getBoundary
        geom.setSRID(lineString.getSRID)
        JTSGeometry(geom)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSLineString = {
        JTSLineString.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[JTSPoint]))
    }

    def asSeq: Seq[JTSPoint] = getShellPoints.head

    override def getHoles: Seq[Seq[JTSLineString]] = Nil

    override def getShells: Seq[JTSLineString] = Seq(this)

    override def flatten: Seq[JTSGeometry] = Seq(this)

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = Nil

}

object JTSLineString extends GeometryReader {

    def getPoints(lineString: LineString): Seq[JTSPoint] = {
        for (i <- 0 until lineString.getNumPoints) yield {
            val point = lineString.getPointN(i)
            point.setSRID(lineString.getSRID)
            new JTSPoint(point)
        }
    }

    override def fromInternal(row: InternalRow): JTSLineString = {
        val internalGeom = InternalGeometry(row)
        val gf = new GeometryFactory()
        val lineString = gf.createLineString(internalGeom.boundaries.head.map(_.toCoordinate))
        lineString.setSRID(internalGeom.srid)
        JTSLineString(lineString)
    }

    override def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = LINESTRING): JTSLineString = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSLineString(gf.createLineString())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                                                =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[JTSPoint])
                gf.createLineString(extractedPoints.map(_.coord).toArray)
            case other: GeometryTypeEnum.Value if other == LINESTRING =>
                throw new Error(
                  s"Joining a sequence of ${other.toString} to create a ${geomType.toString} geometry is not yet supported"
                )
            case other: GeometryTypeEnum.Value                        => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSLineString(newGeom)
    }

    def apply(geometry: Geometry): JTSLineString = {
        GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case LINESTRING => new JTSLineString(geometry.asInstanceOf[LineString])
            case LINEARRING =>
                val newGeom = new GeometryFactory().createLineString(
                  geometry.asInstanceOf[LinearRing].getCoordinates
                )
                newGeom.setSRID(geometry.getSRID)
                new JTSLineString(newGeom)
        }
    }

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
