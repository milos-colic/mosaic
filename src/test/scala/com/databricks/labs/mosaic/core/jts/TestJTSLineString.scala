package com.databricks.labs.mosaic.core.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaRedundantCast
class TestJTSLineString extends AnyFlatSpec {

    "JTSLineString" should "return Nil for holes and hole points calls." in {
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getHoles shouldEqual Nil
        lineString.getHolePoints shouldEqual Nil
    }

    "JTSLineString" should "return seq(this) for shells and flatten calls." in {
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.getShells.head shouldEqual lineString
        lineString.flatten.head shouldEqual lineString
    }

    "JTSLineString" should "return number of points." in {
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        lineString.numPoints shouldEqual 3
    }

    "JTSLineString" should "read all supported formats" in {
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        noException should be thrownBy JTSLineString.fromWKB(lineString.toWKB)
        noException should be thrownBy JTSLineString.fromHEX(lineString.toHEX)
        noException should be thrownBy JTSLineString.fromJSON(lineString.toJSON)
        noException should be thrownBy JTSLineString.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])
        lineString.equals(JTSLineString.fromWKB(lineString.toWKB)) shouldBe true
        lineString.equals(JTSLineString.fromHEX(lineString.toHEX)) shouldBe true
        lineString.equals(JTSLineString.fromJSON(lineString.toJSON)) shouldBe true
        lineString.equals(JTSLineString.fromInternal(lineString.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "JTSLineString" should "be instantiable from a Seq of MosaicPointJTS" in {
        val lineStringReference = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val lineStringTest = JTSLineString.fromSeq(pointsSeq)
        lineStringReference.equals(lineStringTest) shouldBe true
    }

    "JTSLineString" should "not fail for empty Seq" in {
        val expected = JTSLineString.fromWKT(
          "LINESTRING EMPTY"
        )
        val actual = JTSLineString.fromSeq(Seq[JTSLineString]())
        expected.equals(actual) shouldBe true
    }

    "JTSLineString" should "return a Seq of MosaicPointJTS object when calling asSeq" in {
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[JTSLineString]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[JTSPoint])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: JTSPoint, b: JTSPoint) => a.equals(b) }
        results should contain only true
    }

    "JTSLineString" should "return a Seq of MosaicPointJTS object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val lineString = JTSLineString
            .fromWKT("LINESTRING (1 1, 2 2, 3 3)")
            .asInstanceOf[JTSLineString]
        lineString.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        pointsSeqReference.foreach(_.setSpatialReference(srid))
        val pointSeqTest = lineString.asSeq.map(_.asInstanceOf[JTSPoint])

        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: JTSPoint, b: JTSPoint) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "JTSLineString" should "maintain SRID across operations" in {
        val srid = 32632
        val lineString = JTSLineString.fromWKT("LINESTRING (1 1, 2 2, 3 3)").asInstanceOf[JTSLineString]
        val anotherPoint = JTSPoint.fromWKT("POINT(1 1)").asInstanceOf[JTSPoint]
        val poly = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        lineString.setSpatialReference(srid)

        // MosaicGeometryJTS
        lineString.buffer(2d).getSpatialReference shouldBe srid
        lineString.convexHull.getSpatialReference shouldBe srid
        lineString.getCentroid.getSpatialReference shouldBe srid
        lineString.intersection(poly).getSpatialReference shouldBe srid
        lineString.rotate(45).getSpatialReference shouldBe srid
        lineString.scale(2d, 2d).getSpatialReference shouldBe srid
        lineString.simplify(0.001).getSpatialReference shouldBe srid
        lineString.translate(2d, 2d).getSpatialReference shouldBe srid
        lineString.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicLineString
        lineString.asSeq.head.getSpatialReference shouldBe srid
        lineString.flatten.head.getSpatialReference shouldBe srid
        lineString.getShells.head.getSpatialReference shouldBe srid

        // MosaicLineStringJTS
        lineString.getBoundary.getSpatialReference shouldBe srid
        lineString.getShellPoints.head.head.getSpatialReference shouldBe srid
        lineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    "JTSLineString" should "correctly apply CRS transformation" in {
        val sridSource = 4326
        val sridTarget = 27700
        val testLine = JTSLineString
            .fromWKT(
              "LINESTRING(-0.1367293 51.5166525, -0.1370977 51.517082, -0.1380077 51.5186537, -0.1375356 51.518824)"
            )
            .asInstanceOf[JTSLineString]
        testLine.setSpatialReference(sridSource)
        val expectedResult = JTSLineString
            .fromWKT(
              "LINESTRING(529382.90 181393.19, 529356.12 181440.30, 529288.54 181613.47, 529320.81 181633.24)"
            )
            .asInstanceOf[JTSLineString]
        val testResult = testLine.transformCRSXY(sridTarget).asInstanceOf[JTSLineString]
        expectedResult.distance(testResult) should be < 0.001d
    }

}
