package com.databricks.labs.mosaic.core.types.model

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.UUID

case class MosaicRasterChip(isCore: Boolean, index: Either[Long, String], raster: MosaicRaster) {

    def isEmpty: Boolean = !isCore & Option(raster).forall(_.isEmpty)

    def formatCellId(indexSystem: IndexSystem): MosaicRasterChip = {
        (indexSystem.getCellIdDataType, index) match {
            case (_: LongType, Left(value))    => this
            case (_: StringType, Right(value)) => this
            case (_: LongType, Right(value))   => this.copy(index = Left(indexSystem.parse(value)))
            case (_: StringType, Left(value))  => this.copy(index = Right(indexSystem.format(value)))
            case _                             => throw new IllegalArgumentException("Invalid cell id data type")
        }
    }

    def cellIdAsLong(indexSystem: IndexSystem): Long =
        index match {
            case Left(value) => value
            case _           => indexSystem.parse(index.right.get)
        }

    def cellIdAsStr(indexSystem: IndexSystem): String =
        index match {
            case Right(value) => value
            case _            => indexSystem.format(index.left.get)
        }

    /**
      * Serialise to spark internal representation.
      *
      * @return
      *   An instance of [[InternalRow]].
      */
    def serialize: InternalRow = {
        if (index.isLeft) InternalRow.fromSeq(Seq(isCore, index.left.get, encodeGeom))
        else InternalRow.fromSeq(Seq(isCore, UTF8String.fromString(index.right.get), encodeGeom))
    }

    /**
      * Encodes the chip geometry as WKB.
      *
      * @return
      *   An instance of [[Array]] of [[Byte]] representing WKB.
      */
    private def encodeGeom: Array[Byte] = {
        import java.nio.file.{Files, Paths}
        val path = raster.getPath
        if (path.startsWith("/vsimem/")) {
            val driver = raster.getRaster.GetDriver()
            val ext = path.split("\\.").last
            val uuid = UUID.fromString(path)
            val outPath = s"${uuid.toString}.$ext"
            driver.CreateCopy(outPath, raster.getRaster)
            val byteArray = Files.readAllBytes(Paths.get(outPath))
            Files.delete(Paths.get(outPath))
            byteArray
        } else {
            Files.readAllBytes(Paths.get(path))
        }
    }

    def indexAsLong(indexSystem: IndexSystem): Long = {
        if (index.isLeft) index.left.get
        else indexSystem.formatCellId(index.right.get, LongType).asInstanceOf[Long]
    }

}