package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{LINESTRING, MULTILINESTRING}
import com.databricks.labs.mosaic.core.types.model._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._

class JTSMultiLineString(multiLineString: MultiLineString) extends JTSGeometry(multiLineString) {

    override def toInternal: InternalGeometry = {
        val shells = for (i <- 0 until multiLineString.getNumGeometries) yield {
            val lineString = multiLineString.getGeometryN(i).asInstanceOf[LineString]
            lineString.getCoordinates.map(InternalCoord(_))
        }
        new InternalGeometry(MULTILINESTRING.id, getSpatialReference, shells.toArray, Array(Array(Array())))
    }

    override def getBoundary: JTSGeometry = {
        val shellGeom = multiLineString.getBoundary
        shellGeom.setSRID(multiLineString.getSRID)
        JTSGeometry(shellGeom)
    }

    override def getShells: Seq[JTSLineString] =
        for (i <- 0 until multiLineString.getNumGeometries) yield JTSLineString(multiLineString.getGeometryN(i))

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSMultiLineString = {
        JTSMultiLineString.fromSeq(asSeq.map(_.mapXY(f)))
    }

    override def asSeq: Seq[JTSLineString] =
        for (i <- 0 until multiLineString.getNumGeometries) yield {
            val geom = multiLineString.getGeometryN(i).asInstanceOf[LineString]
            geom.setSRID(multiLineString.getSRID)
            new JTSLineString(geom)
        }

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = Nil

    override def getShellPoints: Seq[Seq[JTSPoint]] = getShells.map(_.asSeq)

    override def getHoles: Seq[Seq[JTSLineString]] = Nil

    override def flatten: Seq[JTSGeometry] = asSeq

}

object JTSMultiLineString extends GeometryReader {

    override def fromInternal(row: InternalRow): JTSMultiLineString = {
        val internalGeom = InternalGeometry(row)
        val gf = new GeometryFactory()
        val lineStrings = for (shell <- internalGeom.boundaries) yield gf.createLineString(shell.map(_.toCoordinate))
        val geometry = gf.createMultiLineString(lineStrings)
        geometry.setSRID(internalGeom.srid)
        JTSMultiLineString(geometry)
    }

    override def fromSeq[T <: JTSGeometry](
        geomSeq: Seq[T],
        geomType: GeometryTypeEnum.Value = MULTILINESTRING
    ): JTSMultiLineString = {
        val gf = new GeometryFactory()

        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSMultiLineString(gf.createMultiLineString())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case LINESTRING                    =>
                val extractedLines = geomSeq.map(_.asInstanceOf[JTSLineString])
                gf.createMultiLineString(extractedLines.map(_.getGeom.asInstanceOf[LineString]).toArray)
            // scalastyle:on throwerror
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSMultiLineString(newGeom)
    }

    def apply(geometry: Geometry): JTSMultiLineString = {
        new JTSMultiLineString(geometry.asInstanceOf[MultiLineString])
    }

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
