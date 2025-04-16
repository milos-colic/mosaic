package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index._
import com.databricks.labs.mosaic.expressions.SpatialSQLAPIsMock.st_aswkb
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait PolyfillBehaviors extends MosaicSpatialQueryTest {

    def polyfillOnComputedColumns(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_polyfill(st_aswkb(col("wkt")), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def wktPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_polyfill(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select grid_polyfill(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def wkbPolyfill(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val mc = mosaicContext
        import mc.functions._
        mc.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 9
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(st_aswkb(col("wkt")).as("wkb"))
            .select(grid_polyfill(col("wkb"), resolution))
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select grid_polyfill(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_polyfill(col("wkt"), 3)
        noException should be thrownBy funcs.grid_polyfill(col("wkt"), lit(3))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark
        import sc.implicits._
        val mc = mosaicContext
        mc.register(spark)

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
            case _ => lit("3").expr
        }

        val polyfillExpr = Polyfill(
          lit(wkt).expr,
          resExpr,
          mc.getIndexSystem
        )

        mc.getIndexSystem match {
            case H3IndexSystem  => polyfillExpr.dataType shouldEqual ArrayType(LongType)
            case BNGIndexSystem => polyfillExpr.dataType shouldEqual ArrayType(StringType)
            case _ => polyfillExpr.dataType shouldEqual ArrayType(LongType)
        }

        val badExpr = Polyfill(
          lit(10).expr,
          lit(true).expr,
          mc.getIndexSystem
        )

        an[Error] should be thrownBy badExpr.inputTypes

        noException should be thrownBy polyfillExpr.makeCopy(polyfillExpr.children.toArray)
    }

}
