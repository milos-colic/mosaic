package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import com.databricks.labs.mosaic.core.types.model.{Coordinates, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class JTSPoint(point: Point) extends JTSGeometry(point) {

    def geoCoord: Coordinates = Coordinates(point.getY, point.getX)

    def coord: Coordinate = new Coordinate(point.getX, point.getY)

    def toSeq: Seq[Double] =
        if (point.getCoordinates.length == 2) {
            Seq(getX, getY)
        } else {
            Seq(getX, getY, getZ)
        }

    def getX: Double = point.getX

    def getY: Double = point.getY

    def getZ: Double = point.getCoordinate.z

    def getBoundary: JTSGeometry = {
        val geom = point.getBoundary
        geom.setSRID(point.getSRID)
        JTSGeometry(geom)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry = {
        val (x_, y_) = f(getX, getY)
        JTSPoint(
          new Coordinate(x_, y_),
          point.getSRID
        )
    }

    override def flatten: Seq[JTSPoint] = List(this)

    override def getShellPoints: Seq[Seq[JTSPoint]] = Seq(Seq(this))

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = Nil

    override def getHoles: Seq[Seq[JTSLineString]] = Nil

    override def getShells: Seq[JTSLineString] = Nil

    override def toInternal: InternalGeometry = {
        val shell = point.getCoordinates.map(InternalCoord(_))
        new InternalGeometry(POINT.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def asSeq: Seq[JTSGeometry] = Seq(this)
}

object JTSPoint extends GeometryReader {

    def apply(geoCoord: Coordinates): JTSPoint = {
        this.apply(new Coordinate(geoCoord.lng, geoCoord.lat), defaultSpatialReferenceId)
    }

    def apply(coord: Coordinate, srid: Int): JTSPoint = {
        val gf = new GeometryFactory()
        val point = gf.createPoint(coord)
        point.setSRID(srid)
        new JTSPoint(point)
    }

    def apply(coords: Seq[Double]): JTSPoint = {
        val gf = new GeometryFactory()
        if (coords.length == 3) {
            val point = gf.createPoint(new Coordinate(coords(0), coords(1), coords(2)))
            new JTSPoint(point)
        } else {
            val point = gf.createPoint(new Coordinate(coords(0), coords(1)))
            new JTSPoint(point)
        }
    }

    override def fromInternal(row: InternalRow): JTSPoint = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        val coordinate = internalGeom.boundaries.head.head
        val point = gf.createPoint(coordinate.toCoordinate)
        point.setSRID(internalGeom.srid)
        new JTSPoint(point)
    }

    override def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POINT): JTSGeometry = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSPoint(gf.createPoint())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoint = geomSeq.head.asInstanceOf[JTSPoint]
                gf.createPoint(extractedPoint.coord)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSPoint(newGeom)
    }

    def apply(geom: Geometry): JTSPoint = new JTSPoint(geom.asInstanceOf[Point])

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
