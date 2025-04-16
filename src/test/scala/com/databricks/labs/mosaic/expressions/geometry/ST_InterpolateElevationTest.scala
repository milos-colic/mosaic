package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.jts.JTS
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

case class ST_InterpolateElevationTest() extends QueryTest with SharedSparkSession with ST_InterpolateElevationBehaviours {
    test("Testing ST_InterpolateElevation (H3, JTS) to produce interpolated grid elevations on an unconstrained triangulation") { simpleInterpolationBehavior()}
    test("Testing ST_InterpolateElevation (H3, JTS) to produce interpolated grid elevations on an conforming triangulation") { conformingInterpolationBehavior()}
}
