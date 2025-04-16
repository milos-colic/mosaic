package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum
import org.apache.spark.sql.catalyst.InternalRow

trait GeometryReader {

    val defaultSpatialReferenceId: Int = 4326

    def fromInternal(row: InternalRow): JTSGeometry

    def fromWKB(wkb: Array[Byte]): JTSGeometry

    def fromWKT(wkt: String): JTSGeometry

    def fromJSON(geoJson: String): JTSGeometry

    def fromHEX(hex: String): JTSGeometry

    def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): JTSGeometry

}
