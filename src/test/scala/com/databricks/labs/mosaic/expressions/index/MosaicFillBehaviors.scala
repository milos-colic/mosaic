package com.databricks.labs.mosaic.expressions.index

import com.databricks.labs.mosaic.core.index.{BNGIndexSystem, H3IndexSystem}
import com.databricks.labs.mosaic.core.jts.{JTS, JTSGeometry}
import com.databricks.labs.mosaic.expressions.SpatialSQLAPIsMock.{st_aswkb, st_centroid}
import com.databricks.labs.mosaic.functions.MosaicContext
import com.databricks.labs.mosaic.test.mocks.getBoroughs
import com.databricks.labs.mosaic.test.{MosaicSpatialQueryTest, mocks}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaDeprecation
trait MosaicFillBehaviors extends MosaicSpatialQueryTest {

    def wktMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
              grid_tessellate(col("wkt"), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select grid_tessellate(wkt, $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length

        noException should be thrownBy grid_tessellate(col("wkt"), resolution)
    }

    def wkbMosaicFill(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 4
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
                grid_tessellate(st_aswkb(col("wkt")), resolution)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(convert_to_wkb(wkt), $resolution) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }


    def wktMosaicFillKeepCoreGeom(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

        val boroughs: DataFrame = getBoroughs(mc)

        val mosaics = boroughs
            .select(
                grid_tessellate(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length

        boroughs.createOrReplaceTempView("boroughs")

        val mosaics2 = spark
            .sql(s"""
                    |select mosaicfill(wkt, $resolution, true) from boroughs
                    |""".stripMargin)
            .collect()

        boroughs.collect().length shouldEqual mosaics2.length
    }

    def mosaicFillPoints(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        import mc.functions._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

        val boroughs: DataFrame = getBoroughs(mc)
            .withColumn("centroid", st_centroid(col("wkt")))

        val mosaics = boroughs
            .select(
              grid_tessellate(col("centroid"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def mosaicFillMultiPoints(mosaicContext: MosaicContext): Unit = {
        val sc = spark
        val mc = mosaicContext
        import mc.functions._
        import sc.implicits._
        mosaicContext.register(spark)

        val resolution = mc.getIndexSystem match {
            case H3IndexSystem  => 11
            case BNGIndexSystem => 4
            case _ => 10
        }

        val geometryAPI = JTS

        val boroughs: DataFrame = getBoroughs(mc)
            .select("wkt")
            .as[String]
            .map(wkt => {
                val geom = geometryAPI.geometry(wkt, "WKT")
                val boundaryPoints = geom.getBoundary.getShellPoints.flatten
                val multiPoint = boundaryPoints.tail.fold(boundaryPoints.head.asInstanceOf[JTSGeometry])((mp, p) => mp.union(p))
                multiPoint.toWKT
            })
            .toDF("wkt")

        val mosaics = boroughs
            .select(
                grid_tessellate(col("wkt"), resolution, keepCoreGeometries = true)
            )
            .collect()

        boroughs.collect().length shouldEqual mosaics.length
    }

    def columnFunctionSignatures(mosaicContext: MosaicContext): Unit = {
        val funcs = mosaicContext.functions
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3))
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), 3)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), 3, keepCoreGeometries = true)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3), keepCoreGeometries = true)
        noException should be thrownBy funcs.grid_tessellate(col("wkt"), lit(3), lit(false))
    }

    def auxiliaryMethods(mosaicContext: MosaicContext): Unit = {
        val mc = mosaicContext
        mosaicContext.register(spark)
        val sc = spark
        import sc.implicits._

        val wkt = mocks.getWKTRowsDf(mc.getIndexSystem).limit(1).select("wkt").as[String].collect().head
        val resExpr = mc.getIndexSystem match {
            case H3IndexSystem  => lit(mc.getIndexSystem.resolutions.head).expr
            case BNGIndexSystem => lit("100m").expr
            case _ => lit(4).expr
        }

        val mosaicfillExpr = MosaicFill(
          lit(wkt).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem
        )

        mosaicfillExpr.first shouldEqual lit(wkt).expr
        mosaicfillExpr.second shouldEqual resExpr
        mosaicfillExpr.third shouldEqual lit(false).expr

        mc.getIndexSystem match {
            case H3IndexSystem  => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, IntegerType, BooleanType)
            case BNGIndexSystem => mosaicfillExpr.inputTypes should contain theSameElementsAs
                    Seq(StringType, StringType, BooleanType)
            case _ => mosaicfillExpr.inputTypes should contain theSameElementsAs
              Seq(StringType, IntegerType, BooleanType)
        }

        val badExpr = MosaicFill(
          lit(10).expr,
          resExpr,
          lit(false).expr,
          mc.getIndexSystem
        )

        an[Error] should be thrownBy badExpr.inputTypes
        noException should be thrownBy mosaicfillExpr.makeCopy(mosaicfillExpr.children.toArray)
    }

}
