package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.codegen.format.{GeometryIOCodeGen, MosaicGeometryIOCodeGenJTS}
import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.types.model.{Coordinates, GeometryTypeEnum}
import com.databricks.labs.mosaic.core.types.{HexType, InternalGeometryType, JSONType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

object JTS extends Serializable {

    def createBbox(xMin: Double, yMin: Double, xMax: Double, yMax: Double): JTSGeometry = {
        val p1 = fromGeoCoord(Coordinates(yMin, xMin)).asInstanceOf[JTSPoint]
        val p2 = fromGeoCoord(Coordinates(yMax, xMin)).asInstanceOf[JTSPoint]
        val p3 = fromGeoCoord(Coordinates(yMax, xMax)).asInstanceOf[JTSPoint]
        val p4 = fromGeoCoord(Coordinates(yMin, xMax)).asInstanceOf[JTSPoint]
        geometry(Seq(p1, p2, p3, p4, p1), GeometryTypeEnum.POLYGON)
    }

    def geographicExtent(spatialReferenceID: Int): JTSGeometry = {
        val bounds = CRSBoundsProvider().reprojectedBounds("EPSG", spatialReferenceID)
        createBbox(bounds.lowerLeft.getX, bounds.lowerLeft.getY, bounds.upperRight.getX, bounds.upperRight.getY)
    }

    def geometry(input: Any, typeName: String): JTSGeometry = {
        typeName match {
            case "WKT"     => JTSGeometry.fromWKT(input.asInstanceOf[String])
            case "HEX"     => JTSGeometry.fromHEX(input.asInstanceOf[String])
            case "WKB"     => JTSGeometry.fromWKB(input.asInstanceOf[Array[Byte]])
            case "GEOJSON" => JTSGeometry.fromJSON(input.asInstanceOf[String])
            case "COORDS"  => throw new Error(s"$typeName not supported.")
            case _         => throw new Error(s"$typeName not supported.")
        }
    }

    def geometry(points: Seq[JTSPoint], geomType: GeometryTypeEnum.Value): JTSGeometry = JTSGeometry.fromSeq(points, geomType)

    /**
     * Constructs an instance of [[JTSGeometry]] based on an instance of
     * spark internal data.
     * @param inputData
     *   An instance of [[InternalRow]].
     * @param dataType
     *   A data type of the geometry.
     * @return
     *   An instance of [[JTSGeometry]].
     */
    def geometry(inputData: InternalRow, dataType: DataType): JTSGeometry = {
        dataType match {
            case _: BinaryType           => JTSGeometry.fromWKB(inputData.getBinary(0))
            case _: StringType           => JTSGeometry.fromWKT(inputData.getString(0))
            case _: HexType              => JTSGeometry.fromHEX(inputData.get(0, HexType).asInstanceOf[InternalRow].getString(0))
            case _: JSONType             => JTSGeometry.fromJSON(inputData.get(0, JSONType).asInstanceOf[InternalRow].getString(0))
            case _: InternalGeometryType => JTSGeometry.fromInternal(inputData.get(0, InternalGeometryType).asInstanceOf[InternalRow])
            case _                       => throw new Error(s"$dataType not supported.")
        }
    }

    /**
     * Constructs an instance of [[JTSGeometry]] based on Any instance
     * coming from spark nullSafeEval method.
     * @param inputData
     *   An instance of [[InternalRow]].
     * @param dataType
     *   A data type of the geometry.
     * @return
     *   An instance of [[JTSGeometry]].
     */
    def geometry(inputData: Any, dataType: DataType): JTSGeometry =
        dataType match {
            case _: BinaryType           => JTSGeometry.fromWKB(inputData.asInstanceOf[Array[Byte]])
            case _: StringType           => JTSGeometry.fromWKT(inputData.asInstanceOf[UTF8String].toString)
            case _: HexType              => JTSGeometry.fromHEX(inputData.asInstanceOf[InternalRow].getString(0))
            case _: JSONType             => JTSGeometry.fromJSON(inputData.asInstanceOf[InternalRow].getString(0))
            case _: InternalGeometryType => JTSGeometry.fromInternal(inputData.asInstanceOf[InternalRow])
            case _                       => throw new Error(s"$dataType not supported.")
        }

    def serialize(geometry: JTSGeometry, dataType: DataType): Any = {
        serialize(geometry, GeometryFormat.getDefaultFormat(dataType))
    }

    def serialize(geometry: JTSGeometry, dataFormatName: String): Any = {
        dataFormatName.toUpperCase(Locale.ROOT) match {
            case "WKB"        => geometry.toWKB
            case "WKT"        => UTF8String.fromString(geometry.toWKT)
            case "HEX"        => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toHEX)))
            case "JSONOBJECT" => InternalRow.fromSeq(Seq(UTF8String.fromString(geometry.toJSON)))
            case "GEOJSON"    => UTF8String.fromString(geometry.toJSON)
            case "COORDS"     => geometry.toInternal.serialize
            case _            => throw new Error(s"$dataFormatName not supported.")
        }
    }

    def fromGeoCoord(geoCoord: Coordinates): JTSGeometry = JTSPoint(geoCoord)

    def fromCoords(coords: Seq[Double]): JTSGeometry = JTSPoint(coords)

    def fromSeq(geoms: Seq[JTSGeometry], geomType: GeometryTypeEnum.Value): JTSGeometry = {
        JTSGeometry.fromSeq(geoms, geomType)
    }

    def ioCodeGen: GeometryIOCodeGen = MosaicGeometryIOCodeGenJTS

    def codeGenTryWrap(code: String): String =
        s"""
           |try {
           |$code
           |} catch (Exception e) {
           | throw e;
           |}
           |""".stripMargin

    def geometryClass: String = classOf[JTSGeometry].getName

    def mosaicGeometryClass: String = classOf[JTSGeometry].getName

}
