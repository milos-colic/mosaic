package com.databricks.labs.mosaic.core.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaRedundantCast
class TestJTSPolygon extends AnyFlatSpec {

    "JTSPolygon" should "return Nil for holes and hole points calls." in {
        val point = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        point.getHoles shouldEqual Seq(Nil)
        point.getHolePoints shouldEqual Seq(Nil)
    }

    "JTSPolygon" should "return seq(this) for shells and flatten calls." in {
        val polygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        val lineString = JTSLineString.fromWKT("LINESTRING (0 1,3 0,4 3,0 4,0 1)")
        polygon.getShells.head.equals(lineString) shouldBe true
        polygon.flatten should contain theSameElementsAs Seq(polygon)
    }

    "JTSPolygon" should "return number of points." in {
        val polygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        polygon.numPoints shouldEqual 5
    }

    "JTSPolygon" should "read all supported formats" in {
        val polygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        noException should be thrownBy JTSPolygon.fromWKB(polygon.toWKB)
        noException should be thrownBy JTSPolygon.fromHEX(polygon.toHEX)
        noException should be thrownBy JTSPolygon.fromJSON(polygon.toJSON)
        noException should be thrownBy JTSPolygon.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])
        polygon.equals(JTSPolygon.fromWKB(polygon.toWKB)) shouldBe true
        polygon.equals(JTSPolygon.fromHEX(polygon.toHEX)) shouldBe true
        polygon.equals(JTSPolygon.fromJSON(polygon.toJSON)) shouldBe true
        polygon.equals(JTSPolygon.fromInternal(polygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "JTSPolygon" should "be instantiable from a Seq of JTSPoint" in {
        val polygonReference = JTSPolygon.fromWKT("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")
        val pointSeq_open = Seq("POINT (30 10)", "POINT (40 40)", "POINT (20 40)", "POINT (10 20)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val pointSeq_closed = Seq("POINT (30 10)", "POINT (40 40)", "POINT (20 40)", "POINT (10 20)", "POINT (30 10)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val polygonTest_open = JTSPolygon.fromSeq(pointSeq_open)
        val polygonTest_closed = JTSPolygon.fromSeq(pointSeq_closed)
        polygonReference.equals(polygonTest_open) shouldBe true
        polygonReference.equals(polygonTest_closed) shouldBe true
    }

    "JTSPolygon" should "not fail for empty Seq" in {
        val expected = JTSPolygon.fromWKT(
            "POLYGON EMPTY"
        )
        val actual = JTSPolygon.fromSeq(Seq[JTSPolygon]())
        expected.equals(actual) shouldBe true
    }

    "JTSPolygon" should "be instantiable from a Seq of JTSLineString" in {
        val polygonReference = JTSPolygon.fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
        val linesSeq_open = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20)", "LINESTRING (20 30, 35 35, 30 20)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSLineString])
        val linesSeq_closed = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSLineString])
        val polygonTest_open = JTSPolygon.fromSeq(linesSeq_open)
        val polygonTest_closed = JTSPolygon.fromSeq(linesSeq_closed)
        polygonReference.equals(polygonTest_open) shouldBe true
        polygonReference.equals(polygonTest_closed) shouldBe true
    }

    "JTSPolygon" should "return a Seq of JTSLineString object when calling asSeq" in {
        val polygon = JTSPolygon
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[JTSPolygon]
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSLineString])
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[JTSLineString])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: JTSLineString, b: JTSLineString) => a.equals(b) }
        results should contain only true
    }

    "JTSPolygon" should "return a Seq of JTSLineString object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val polygon = JTSPolygon
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[JTSPolygon]
        polygon.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (35 10, 45 45, 15 40, 10 20, 35 10)", "LINESTRING (20 30, 35 35, 30 20, 20 30)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSLineString])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = polygon.asSeq.map(_.asInstanceOf[JTSLineString])
        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: JTSLineString, b: JTSLineString) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "JTSPolygon" should "maintain SRID across operations" in {
        val srid = 32632
        val polygon = JTSPolygon
            .fromWKT("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))")
            .asInstanceOf[JTSPolygon]
        val otherPolygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        polygon.setSpatialReference(srid)

        // MosaicGeometryJTS
        polygon.buffer(2d).getSpatialReference shouldBe srid
        polygon.convexHull.getSpatialReference shouldBe srid
        polygon.getCentroid.getSpatialReference shouldBe srid
        polygon.intersection(otherPolygon).getSpatialReference shouldBe srid
        polygon.rotate(45).getSpatialReference shouldBe srid
        polygon.scale(2d, 2d).getSpatialReference shouldBe srid
        polygon.simplify(0.001).getSpatialReference shouldBe srid
        polygon.translate(2d, 2d).getSpatialReference shouldBe srid
        polygon.union(otherPolygon).getSpatialReference shouldBe srid

        // MosaicPolygon
        polygon.flatten.head.getSpatialReference shouldBe srid
        polygon.getShellPoints.head.head.getSpatialReference shouldBe srid
        polygon.getHolePoints.head.head.head.getSpatialReference shouldBe srid

        // JTSPolygon
        polygon.asSeq.head.getSpatialReference shouldBe srid
        polygon.getBoundary.getSpatialReference shouldBe srid
        polygon.getHoles.head.head.getSpatialReference shouldBe srid
        polygon.getShells.head.getSpatialReference shouldBe srid
        polygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "JTSPolygon" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testPolygon = JTSPolygon
            .fromWKT(
              "POLYGON((-0.1367293 51.5166525, -0.1370977 51.517082, -0.1380077 51.5186537, -0.1375356 51.518824, -0.1371474 51.5184174, -0.1361386 51.5167553, -0.1367293 51.5166525))"
            )
            .asInstanceOf[JTSPolygon]
        testPolygon.setSpatialReference(sridSource)
        val expectedResult = JTSPolygon
            .fromWKT(
              "POLYGON((529382.90 181393.19, 529356.12 181440.30, 529288.54 181613.47, 529320.81 181633.24, 529348.89 181588.71, 529423.59 181405.67, 529382.90 181393.19))"
            )
            .asInstanceOf[JTSPolygon]
        val testResult = testPolygon.transformCRSXY(sridTarget).asInstanceOf[JTSPolygon]
        val intersection = expectedResult.intersection(testResult)
        intersection.getArea shouldBe expectedResult.getArea +- 1d
    }

}
