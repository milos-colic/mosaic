package com.databricks.labs.mosaic.core.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaRedundantCast
class TestJTSMultiLineString extends AnyFlatSpec {

    "JTSMultiLineString" should "return Nil for holes and hole points calls." in {
        val multiLineString = JTSMultiLineString.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.getHoles shouldEqual Nil
        multiLineString.getHolePoints shouldEqual Nil
    }

    "JTSMultiLineString" should "return seq(this) for shells and flatten calls." in {
        val multiLineString = JTSMultiLineString.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        val lineString = JTSLineString.fromWKT("LINESTRING(1 1, 2 2, 3 3)")
        multiLineString.getShells.head.equals(lineString) shouldBe true
        multiLineString.flatten.head.equals(lineString) shouldBe true
    }

    "JTSMultiLineString" should "return number of points." in {
        val multiLineString = JTSMultiLineString.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        multiLineString.numPoints shouldEqual 6
    }

    "JTSMultiLineString" should "read all supported formats" in {
        val multiLineString = JTSMultiLineString.fromWKT("MULTILINESTRING ((1 1, 2 2, 3 3), (2 2, 3 3, 4 4))")
        noException should be thrownBy JTSMultiLineString.fromWKB(multiLineString.toWKB)
        noException should be thrownBy JTSMultiLineString.fromHEX(multiLineString.toHEX)
        noException should be thrownBy JTSMultiLineString.fromJSON(multiLineString.toJSON)
        noException should be thrownBy JTSMultiLineString.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        multiLineString.equals(JTSMultiLineString.fromWKB(multiLineString.toWKB)) shouldBe true
        multiLineString.equals(JTSMultiLineString.fromHEX(multiLineString.toHEX)) shouldBe true
        multiLineString.equals(JTSMultiLineString.fromJSON(multiLineString.toJSON)) shouldBe true
        multiLineString.equals(
            JTSMultiLineString.fromInternal(multiLineString.toInternal.serialize.asInstanceOf[InternalRow])
        ) shouldBe true
    }

    "JTSMultiLineString" should "be instantiable from a Seq of MosaicLineStringJTS" in {
        val multiLineStringReference =
            JTSMultiLineString.fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
        val multiLinesSeq = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(JTSMultiLineString.fromWKT)
            .map(_.asInstanceOf[JTSMultiLineString])
        val multiLineStringTest = JTSMultiLineString.fromSeq(multiLinesSeq)
        multiLineStringReference.equals(multiLineStringTest) shouldBe true
    }

    "JTSMultiLineString" should "not fail for empty Seq" in {
        val expected = JTSMultiLineString.fromWKT(
            "MULTILINESTRING EMPTY"
        )
        val actual = JTSMultiLineString.fromSeq(Seq[JTSMultiLineString]())
        expected.equals(actual) shouldBe true
    }

    "JTSMultiLineString" should "return a Seq of JTSMultiLineString object when calling asSeq" in {
        val multiLineString = JTSMultiLineString
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[JTSMultiLineString]
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(JTSMultiLineString.fromWKT)
            .map(_.asInstanceOf[JTSMultiLineString])
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[JTSMultiLineString])
        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: JTSMultiLineString, b: JTSMultiLineString) => a.equals(b) }
        results should contain only true
    }

    "JTSMultiLineString" should "return a Seq of JTSMultiLineString object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiLineString = JTSMultiLineString
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[JTSMultiLineString]
        multiLineString.setSpatialReference(srid)
        val linesSeqReference = Seq("LINESTRING (10 10, 20 20, 10 40)", "LINESTRING (40 40, 30 30, 40 20, 30 10)")
            .map(JTSLineString.fromWKT)
            .map(_.asInstanceOf[JTSLineString])
        linesSeqReference.foreach(_.setSpatialReference(srid))
        val lineSeqTest = multiLineString.asSeq.map(_.asInstanceOf[JTSLineString])

        lineSeqTest.map(_.getSpatialReference) should contain only srid

        val results = linesSeqReference
            .zip(lineSeqTest)
            .map { case (a: JTSLineString, b: JTSLineString) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "JTSMultiLineString" should "maintain SRID across operations" in {
        val srid = 32632
        val multiLineString = JTSMultiLineString
            .fromWKT("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))")
            .asInstanceOf[JTSMultiLineString]
        val anotherLine = JTSLineString.fromWKT("LINESTRING (20 10, 20 20, 20 40)").asInstanceOf[JTSLineString]
        val poly = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiLineString.setSpatialReference(srid)

        // MosaicGeometryJTS
        multiLineString.buffer(2d).getSpatialReference shouldBe srid
        multiLineString.convexHull.getSpatialReference shouldBe srid
        multiLineString.getCentroid.getSpatialReference shouldBe srid
        multiLineString.intersection(poly).getSpatialReference shouldBe srid
        multiLineString.rotate(45).getSpatialReference shouldBe srid
        multiLineString.scale(2d, 2d).getSpatialReference shouldBe srid
        multiLineString.simplify(0.001).getSpatialReference shouldBe srid
        multiLineString.translate(2d, 2d).getSpatialReference shouldBe srid
        multiLineString.union(anotherLine).getSpatialReference shouldBe srid

        // MosaicMultiLineString
        multiLineString.flatten.head.getSpatialReference shouldBe srid
        multiLineString.getShellPoints.head.head.getSpatialReference shouldBe srid

        // MosaicMultiLineStringJTS
        multiLineString.asSeq.head.getSpatialReference shouldBe srid
        multiLineString.getBoundary.getSpatialReference shouldBe srid
        multiLineString.getShells.head.getSpatialReference shouldBe srid
        multiLineString.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
