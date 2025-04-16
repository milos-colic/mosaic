package com.databricks.labs.mosaic.core.geometry

import com.databricks.labs.mosaic.core.jts.JTSGeometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Test_MosaicGeometryJTS extends AnyFunSuite {

    test("Test_MosaicGeometryJTS Line Intersection - Issue 299") {
        val verticalLine = """{"type":"LineString","coordinates":[[1,0], [1,2]]}"""
        val horizontalLine = """{"type": "LineString", "coordinates": [[0,1], [2,1]]}"""

        val verticalLineGeom = JTSGeometry.fromJSON(verticalLine)
        val horizontalLineGeom = JTSGeometry.fromJSON(horizontalLine)

        val intersection = verticalLineGeom.intersection(horizontalLineGeom)
        intersection.isEmpty shouldBe false

    }

}
