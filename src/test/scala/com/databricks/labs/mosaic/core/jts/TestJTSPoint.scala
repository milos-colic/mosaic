package com.databricks.labs.mosaic.core.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestJTSPoint extends AnyFlatSpec {

    "JTSPoint" should "return Nil for holes and hole points calls." in {
        val point = JTSPoint.fromWKT("POINT(1 1)")
        point.getHoles shouldEqual Nil
        point.getHolePoints shouldEqual Nil
    }

    "JTSPoint" should "return seq(this) for shells and flatten calls." in {
        val point = JTSPoint.fromWKT("POINT(1 1)")
        the[Exception] thrownBy
            point.getShells should have
        message("getShells should not be called on MultiPoints.")
        point.flatten should contain theSameElementsAs Seq(point)
    }

    "JTSPoint" should "return number of points." in {
        val point = JTSPoint.fromWKT("POINT(1 1)")
        point.numPoints shouldEqual 1
    }

    "JTSPoint" should "be instantiable from a Seq of JTSPoint" in {
        val lineStringReference = JTSPoint.fromWKT("POINT (1 1)")
        val pointsSeq = Seq("POINT (1 1)")
          .map(JTSPoint.fromWKT)
          .map(_.asInstanceOf[JTSPoint])
        val lineStringTest = JTSPoint.fromSeq(pointsSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

    "JTSPoint" should "not fail for empty Seq" in {
        val expected = JTSPoint.fromWKT(
            "POINT EMPTY"
        )
        val actual = JTSPoint.fromSeq(Seq())
        expected.equals(actual) shouldBe true
    }

    "JTSPoint" should "read all supported formats" in {
        val point = JTSPoint.fromWKT("POINT(1 1)")
        noException should be thrownBy JTSPoint.fromWKB(point.toWKB)
        noException should be thrownBy JTSPoint.fromHEX(point.toHEX)
        noException should be thrownBy JTSPoint.fromJSON(point.toJSON)
        noException should be thrownBy JTSPoint.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])
        point.equals(JTSPoint.fromWKB(point.toWKB)) shouldBe true
        point.equals(JTSPoint.fromHEX(point.toHEX)) shouldBe true
        point.equals(JTSPoint.fromJSON(point.toJSON)) shouldBe true
        point.equals(JTSPoint.fromInternal(point.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "JTSPoint" should "maintain SRID across operations" in {
        val srid = 32632
        val point = JTSPoint.fromWKT("POINT(1 1)").asInstanceOf[JTSPoint]
        val anotherPoint = JTSPoint.fromWKT("POINT(1 1)").asInstanceOf[JTSPoint]
        val poly = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        point.setSpatialReference(srid)

        // MosaicGeometryJTS
        point.buffer(2d).getSpatialReference shouldBe srid
        point.convexHull.getSpatialReference shouldBe srid
        point.getCentroid.getSpatialReference shouldBe srid
        point.intersection(poly).getSpatialReference shouldBe srid
        point.rotate(45).getSpatialReference shouldBe srid
        point.scale(2d, 2d).getSpatialReference shouldBe srid
        point.simplify(0.001).getSpatialReference shouldBe srid
        point.translate(2d, 2d).getSpatialReference shouldBe srid
        point.union(anotherPoint).getSpatialReference shouldBe srid

        // JTSPoint
        point.getBoundary.getSpatialReference shouldBe srid
        point.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "JTSPoint" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testPoint = JTSPoint.fromWKT("POINT(-0.1390688 51.5178267)").asInstanceOf[JTSPoint]
        testPoint.setSpatialReference(sridSource)
        val expectedResult = JTSPoint.fromWKT("POINT(529217.26 181519.64)").asInstanceOf[JTSPoint]
        val testResult = testPoint.transformCRSXY(sridTarget).asInstanceOf[JTSPoint]
        val comparison = {
            expectedResult.toSeq
                .zip(testResult.toSeq)
                .map({ case (a: Double, b: Double) => math.abs(a - b) <= 0.01 })
        }
        comparison.take(2) should contain only true
    }

}
