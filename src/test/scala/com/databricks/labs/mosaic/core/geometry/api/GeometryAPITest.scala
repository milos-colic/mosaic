package com.databricks.labs.mosaic.core.geometry.api

import com.databricks.labs.mosaic.codegen.format.MosaicGeometryIOCodeGenJTS
import com.databricks.labs.mosaic.core.jts.{JTS, JTSPoint}
import com.databricks.labs.mosaic.core.types.model.Coordinates
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryAPITest extends AnyFunSuite with GeometryAPIBehaviors {


    test("JTS Geometry API") {
        val jts = JTS
        jts.fromCoords(Seq(0.1, 0.2)).equals(JTSPoint(Seq(0.1, 0.2))) shouldEqual true
        jts.fromGeoCoord(Coordinates(0.2, 0.1)).equals(JTSPoint(Seq(0.1, 0.2))) shouldEqual true
        jts.ioCodeGen shouldEqual MosaicGeometryIOCodeGenJTS
        jts.codeGenTryWrap("1==1;").contains("try") shouldEqual true
    }


    test("Geometry API serialize and deserialize") {
        serializeDeserializeBehavior(JTSPoint(Seq(0.1, 0.2)))
    }

    test("Geometry API throw an exception when serializing non existing format.") {
        val point = JTSPoint.fromWKT("POINT(1 1)")
        val geometryAPI = JTS

        assertThrows[Error] {
            geometryAPI.serialize(point, "non-existent-format")
        }
    }

    test("Base signatures") {
        val jts = JTS
        noException should be thrownBy jts.fromCoords(Seq(0.1, 0.2))
        noException should be thrownBy jts.fromGeoCoord(Coordinates(0.2, 0.1))
        noException should be thrownBy jts.ioCodeGen
        noException should be thrownBy jts.codeGenTryWrap("1==1;")
        noException should be thrownBy jts.geometryClass
        noException should be thrownBy jts.mosaicGeometryClass
    }

}
