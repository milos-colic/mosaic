package com.databricks.labs.mosaic.core.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class TestJTSMultiPolygon extends AnyFlatSpec {

    "JTSMultiPolygon" should "return Nil for holes and hole points calls." in {
        val multiPolygon = JTSMultiPolygon.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.getHoles should contain theSameElementsAs Seq(Nil, Nil)
        multiPolygon.getHolePoints should contain theSameElementsAs Seq(Nil, Nil)
    }

    "JTSMultiPolygon" should "return seq(this) for shells and flatten calls." in {
        val multiPolygon = JTSMultiPolygon.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        val polygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")
        multiPolygon.getShells.head.equals(polygon.getShells.head) shouldBe true
        multiPolygon.flatten.head.equals(polygon) shouldBe true
    }

    "JTSMultiPolygon" should "return number of points." in {
        val multiPolygon = JTSMultiPolygon.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        multiPolygon.numPoints shouldEqual 9
    }

    "JTSMultiPolygon" should "read all supported formats" in {
        val multiPolygon = JTSMultiPolygon.fromWKT("MULTIPOLYGON(((0 1,3 0,4 3,0 4,0 1)), ((3 4,6 3,5 5,3 4)))")
        noException should be thrownBy JTSMultiPolygon.fromWKB(multiPolygon.toWKB)
        noException should be thrownBy JTSMultiPolygon.fromHEX(multiPolygon.toHEX)
        noException should be thrownBy JTSMultiPolygon.fromJSON(multiPolygon.toJSON)
        noException should be thrownBy JTSMultiPolygon.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])
        multiPolygon.equals(JTSMultiPolygon.fromWKB(multiPolygon.toWKB)) shouldBe true
        multiPolygon.equals(JTSMultiPolygon.fromHEX(multiPolygon.toHEX)) shouldBe true
        multiPolygon.equals(JTSMultiPolygon.fromJSON(multiPolygon.toJSON)) shouldBe true
        multiPolygon.equals(JTSMultiPolygon.fromInternal(multiPolygon.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "JTSMultiPolygon" should "be instantiable from a Seq of JTSPolygon" in {
        val multiPolygonReference = JTSMultiPolygon.fromWKT(
          "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
        )
        val polygonSeq = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(JTSPolygon.fromWKT)
            .map(_.asInstanceOf[JTSPolygon])
        val multiPolygonTest = JTSMultiPolygon.fromSeq(polygonSeq)
        multiPolygonReference.equals(multiPolygonTest) shouldBe true
    }

    "JTSMultiPolygon" should "not fail for empty Seq" in {
        val expected = JTSMultiPolygon.fromWKT(
            "MULTIPOLYGON EMPTY"
        )
        val actual = JTSMultiPolygon.fromSeq(Seq[JTSPolygon]())
        expected.equals(actual) shouldBe true
    }

    "JTSMultiPolygon" should "return a Seq of JTSPolygon object when calling asSeq" in {
        val multiPolygon = JTSMultiPolygon
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[JTSMultiPolygon]
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(JTSPolygon.fromWKT)
            .map(_.asInstanceOf[JTSPolygon])
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[JTSPolygon])
        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: JTSPolygon, b: JTSPolygon) => a.equals(b) }
        results should contain only true
    }

    "JTSMultiPolygon" should "return a Seq of JTSPolygon objects with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPolygon = JTSMultiPolygon
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[JTSMultiPolygon]
        multiPolygon.setSpatialReference(srid)
        val polygonSeqReference = Seq(
          "POLYGON ((40 40, 20 45, 45 30, 40 40))",
          "POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20))"
        )
            .map(JTSPolygon.fromWKT)
            .map(_.asInstanceOf[JTSPolygon])
        polygonSeqReference.foreach(_.setSpatialReference(srid))
        val polygonSeqTest = multiPolygon.asSeq.map(_.asInstanceOf[JTSPolygon])
        polygonSeqTest.map(_.getSpatialReference) should contain only srid

        val results = polygonSeqReference
            .zip(polygonSeqTest)
            .map { case (a: JTSPolygon, b: JTSPolygon) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "JTSPolygon" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPolygon = JTSMultiPolygon
            .fromWKT(
              "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"
            )
            .asInstanceOf[JTSMultiPolygon]
        val otherPolygon = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPolygon.setSpatialReference(srid)

        // MosaicGeometryJTS
        multiPolygon.buffer(2d).getSpatialReference shouldBe srid
        multiPolygon.convexHull.getSpatialReference shouldBe srid
        multiPolygon.getCentroid.getSpatialReference shouldBe srid
        multiPolygon.intersection(otherPolygon).getSpatialReference shouldBe srid
        multiPolygon.rotate(45).getSpatialReference shouldBe srid
        multiPolygon.scale(2d, 2d).getSpatialReference shouldBe srid
        multiPolygon.simplify(0.001).getSpatialReference shouldBe srid
        multiPolygon.translate(2d, 2d).getSpatialReference shouldBe srid
        multiPolygon.union(otherPolygon).getSpatialReference shouldBe srid

        // MosaicMultiPolygon
        multiPolygon.flatten.head.getSpatialReference shouldBe srid
        multiPolygon.getShellPoints.head.head.getSpatialReference shouldBe srid
        multiPolygon.getHolePoints.last.head.head.getSpatialReference shouldBe srid

        // JTSMultiPolygon
        multiPolygon.asSeq.head.getSpatialReference shouldBe srid
        multiPolygon.getBoundary.getSpatialReference shouldBe srid
        multiPolygon.getHoles.last.head.getSpatialReference shouldBe srid
        multiPolygon.getShells.head.getSpatialReference shouldBe srid
        multiPolygon.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

}
