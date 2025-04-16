package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.jts.JTSGeometry
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.lit
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers.{an, be, convertToAnyShouldWrapper}

trait ST_BufferLoopBehaviors extends MosaicSpatialQueryTest {

    def behavior(mc: MosaicContext): Unit = {
        val sc = spark
        mc.register(sc)
        import mc.functions._
        import sc.implicits._

        val buffer_udf = functions.udf((wkt: String, distance1: Double) => {
            val geom = JTSGeometry.fromWKT(wkt)
            val geom1 = geom.buffer(distance1)
            geom1.toWKT
        })

        val difference_udf = functions.udf((wkt: String, wkt2: String) => {
            val geom1 = JTSGeometry.fromWKT(wkt)
            val geom2 = JTSGeometry.fromWKT(wkt2)
            geom1.difference(geom2)
        })

        val result = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .select("wkt")
            .withColumn("wkt", st_bufferloop($"wkt", lit(0.1), lit(0.2)))

        val expected = mocks
            .getWKTRowsDf()
            .orderBy("id")
            .withColumn("wkt1", buffer_udf($"wkt", lit(0.1)))
            .withColumn("wkt2", buffer_udf($"wkt", lit(0.2)))
            .withColumn("wkt", difference_udf($"wkt2", $"wkt1"))
            .select("wkt")

        checkGeometryTopo(mc, result, expected, "wkt")
    }

    def codegenCompilation(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)
        import mc.functions._
        import sc.implicits._

        val result = mocks.getWKTRowsDf().select(st_bufferloop($"wkt", 0.1, 0.2))

        val plan = result.queryExecution.executedPlan
        val wholeStageCodegenExec = plan.find(_.isInstanceOf[WholeStageCodegenExec])
        wholeStageCodegenExec.isDefined shouldBe true

        val codeGenStage = wholeStageCodegenExec.get.asInstanceOf[WholeStageCodegenExec]
        val (_, code) = codeGenStage.doCodeGen()
        noException should be thrownBy CodeGenerator.compile(code)

        val stEnvelope = ST_BufferLoop(lit(1).expr, lit(1).expr, lit(1).expr, mc.expressionConfig)
        val ctx = new CodegenContext
        an[Error] should be thrownBy stEnvelope.genCode(ctx)
    }

    def auxiliaryMethods(mc: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")

        val sc = spark
        mc.register(sc)

        val input = "POLYGON (10 10, 20 10, 15 20, 10 10)"

        val stBufferLoop =  ST_BufferLoop(lit(input).expr, lit(0.1).expr, lit(0.2).expr, mc.expressionConfig)
        stBufferLoop.first shouldEqual lit(input).expr
        stBufferLoop.second shouldEqual lit(0.1).expr
        stBufferLoop.third shouldEqual lit(0.2).expr
        noException should be thrownBy stBufferLoop.makeCopy(Array(stBufferLoop.first, stBufferLoop.second, stBufferLoop.third))
    }

}
