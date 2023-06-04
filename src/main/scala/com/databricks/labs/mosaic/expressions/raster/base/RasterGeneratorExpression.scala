package com.databricks.labs.mosaic.expressions.raster.base

import com.databricks.labs.mosaic.MOSAIC_RASTER_STORAGE_DISK
import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.raster.MosaicRaster
import com.databricks.labs.mosaic.core.raster.api.RasterAPI
import com.databricks.labs.mosaic.expressions.base.GenericExpressionFactory
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

/**
  * Base class for all raster generator expressions that take no arguments. It
  * provides the boilerplate code needed to create a function builder for a
  * given expression. It minimises amount of code needed to create a new
  * expression. These expressions are used to generate a collection of new
  * rasters based on the input raster. The new rasters are written in the
  * checkpoint directory. The files are written as GeoTiffs. Subdatasets are not
  * supported, please flatten beforehand.
  * @param inPathExpr
  *   The expression for the raster path.
  * @param expressionConfig
  *   Additional arguments for the expression (expressionConfigs).
  * @tparam T
  *   The type of the extending class.
  */
abstract class RasterGeneratorExpression[T <: Expression: ClassTag](
    inPathExpr: Expression,
    expressionConfig: MosaicExpressionConfig
) extends CollectionGenerator
      with NullIntolerant
      with Serializable {

    val uuid: String = java.util.UUID.randomUUID().toString.replace("-", "_")

    /**
      * The raster API to be used. Enable the raster so that subclasses dont
      * need to worry about this.
      */
    protected val rasterAPI: RasterAPI = RasterAPI(expressionConfig.getRasterAPI)
    rasterAPI.enable()
    protected val geometryAPI: GeometryAPI = GeometryAPI.apply(expressionConfig.getGeometryAPI)

    override def position: Boolean = false

    override def inline: Boolean = false

    /**
      * Generators expressions require an abstraction for element type. Always
      * needs to be wrapped in a StructType. The actually type is that of the
      * structs element.
      */
    override def elementSchema: StructType = {
        val rasterStorage = expressionConfig.getRasterStorage
        if (rasterStorage == MOSAIC_RASTER_STORAGE_DISK) {
            StructType(Array(StructField("path", StringType)))
        } else {
            StructType(Array(StructField("raster", BinaryType)))
        }
    }

    /**
      * The function to be overridden by the extending class. It is called when
      * the expression is evaluated. It provides the raster band to the
      * expression. It abstracts spark serialization from the caller.
      * @param raster
      *   The raster to be used.
      * @return
      *   Sequence of generated new rasters to be written.
      */
    def rasterGenerator(raster: MosaicRaster): Seq[MosaicRaster]

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val inPath = inPathExpr.eval(input).asInstanceOf[UTF8String].toString
        val checkpointPath = expressionConfig.getRasterCheckpoint
        val rasterStorage = expressionConfig.getRasterStorage

        val inRaster = rasterAPI.raster(inPath)
        val generatedRasters = rasterGenerator(inRaster)

        val rows = rasterAPI.writeRasters(generatedRasters, checkpointPath, rasterStorage)
        generatedRasters.foreach(_.unlink())
        rows
    }

    override def makeCopy(newArgs: Array[AnyRef]): Expression =
        GenericExpressionFactory.makeCopyImpl[T](this, newArgs, children.length, expressionConfig)

    override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = makeCopy(newChildren.toArray)

}