package com.databricks.labs.mosaic.expressions.geometry

import com.databricks.labs.mosaic.core.index.H3IndexSystem
import com.databricks.labs.mosaic.core.jts.JTS
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ST_TriangulateTest extends QueryTest with SharedSparkSession with ST_TriangulateBehaviours {
    test("Testing ST_Triangulate (H3, JTS) to produce unconstrained triangulation") { simpleTriangulateBehavior(H3IndexSystem)}
    test("Testing ST_Triangulate (H3, JTS) to produce conforming triangulation") { conformingTriangulateBehavior(H3IndexSystem)}
}
