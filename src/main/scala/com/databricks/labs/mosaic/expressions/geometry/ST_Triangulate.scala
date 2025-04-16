package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.jts.{JTS, JTSLineString, JTSMultiPoint, JTSPoint}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum
import com.databricks.labs.mosaic.expressions.base.{GenericExpressionFactory, WithExpressionInfo}
import com.databricks.labs.mosaic.functions.MosaicExpressionConfig
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

case class ST_Triangulate (
                              pointsArray: Expression,
                              linesArray: Expression,
                              mergeTolerance: Expression,
                              snapTolerance: Expression,
                              splitPointFinder: Expression,
                              expressionConfig: MosaicExpressionConfig
                          )
    extends CollectionGenerator
    with Serializable
    with CodegenFallback {


    override def position: Boolean = false

    override def inline: Boolean = false

    override def elementSchema: StructType = StructType(Seq(StructField("triangles", firstElementType)))

    def firstElementType: DataType = pointsArray.dataType.asInstanceOf[ArrayType].elementType

    def secondElementType: DataType = linesArray.dataType.asInstanceOf[ArrayType].elementType

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
        val pointsGeom =
            pointsArray
                .eval(input)
                .asInstanceOf[ArrayData]
                .toObjectArray(firstElementType)
                .map({
                    obj =>
                        val g = JTS.geometry(obj, firstElementType)
                        g.getGeometryType.toUpperCase(Locale.ROOT) match {
                            case "POINT" => g.asInstanceOf[JTSPoint]
                            case _ => throw new UnsupportedOperationException("ST_Triangulate requires Point geometry as mass points input")
                        }
                })

        val multiPointGeom = JTS.fromSeq(pointsGeom, MULTIPOINT).asInstanceOf[JTSMultiPoint]
        val linesGeom =
            linesArray
                .eval(input)
                .asInstanceOf[ArrayData]
                .toObjectArray(secondElementType)
                .map({
                    obj =>
                        val g = JTS.geometry(obj, secondElementType)
                            g.getGeometryType.toUpperCase(Locale.ROOT) match {
                                case "LINESTRING" => g.asInstanceOf[JTSLineString]
                                case _ => throw new UnsupportedOperationException("ST_Triangulate requires LINESTRING geometry as breakline input")
                            }
                })

        val mergeToleranceVal = mergeTolerance.eval(input).asInstanceOf[Double]
        val snapToleranceVal = snapTolerance.eval(input).asInstanceOf[Double]
        val splitPointFinderVal =
            TriangulationSplitPointTypeEnum.fromString(splitPointFinder.eval(input).asInstanceOf[UTF8String].toString)

        val triangles =  multiPointGeom.triangulate(linesGeom, mergeToleranceVal, snapToleranceVal, splitPointFinderVal)

        val outputGeoms = triangles.map(
            JTS.serialize(_, firstElementType)
        )
        val outputRows = outputGeoms.map(t => InternalRow.fromSeq(Seq(t)))
        outputRows
    }

    override def children: Seq[Expression] =
        Seq(
            pointsArray, linesArray,
            mergeTolerance, snapTolerance, splitPointFinder
        )

    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
        copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3), newChildren(4))
}


object ST_Triangulate extends WithExpressionInfo {

    override def name: String = "st_triangulate"

    override def usage: String = "_FUNC_(expr1, expr2, expr3, expr4, expr5) - Returns the triangulated irregular network " +
        "of the points in `expr1` including `expr2` as breaklines with tolerance parameters `expr3` and `expr4` " +
        "employing the split point insertion algorithm `expr5`."

    override def example: String =
        """
          |    Examples:
          |      > SELECT _FUNC_(a, b, c, d, e);
          |        Point Z (...)
          |        Point Z (...)
          |        ...
          |        Point Z (...)
          |  """.stripMargin

    override def builder(expressionConfig: MosaicExpressionConfig): FunctionBuilder = {
        GenericExpressionFactory.getBaseBuilder[ST_Triangulate](5, expressionConfig)
    }

}
