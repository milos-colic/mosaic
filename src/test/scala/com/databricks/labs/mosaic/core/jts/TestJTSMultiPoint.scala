package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.types.model.TriangulationSplitPointTypeEnum
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

//noinspection ScalaRedundantCast
class TestJTSMultiPoint extends AnyFlatSpec {

    "JTSMultiPoint" should "return Nil for holes and hole points calls." in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.getHoles shouldEqual Nil
        multiPoint.getHolePoints shouldEqual Nil
    }

    "JTSMultiPoint" should "return seq(this) for shells and flatten calls." in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val point = JTSPoint.fromWKT("POINT (1 1)")
        the[Exception] thrownBy
            multiPoint.getShells.head.equals(point) should have
        message("getShells should not be called on MultiPoints.")
        multiPoint.flatten.head.equals(point) shouldBe true
    }

    "JTSMultiPoint" should "return number of points." in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        multiPoint.numPoints shouldEqual 3
    }

    "JTSMultiPoint" should "read all supported formats" in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        noException should be thrownBy JTSMultiPoint.fromWKB(multiPoint.toWKB)
        noException should be thrownBy JTSMultiPoint.fromHEX(multiPoint.toHEX)
        noException should be thrownBy JTSMultiPoint.fromJSON(multiPoint.toJSON)
        noException should be thrownBy JTSMultiPoint.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])
        multiPoint.equals(JTSMultiPoint.fromWKB(multiPoint.toWKB)) shouldBe true
        multiPoint.equals(JTSMultiPoint.fromHEX(multiPoint.toHEX)) shouldBe true
        multiPoint.equals(JTSMultiPoint.fromJSON(multiPoint.toJSON)) shouldBe true
        multiPoint.equals(JTSMultiPoint.fromInternal(multiPoint.toInternal.serialize.asInstanceOf[InternalRow])) shouldBe true
    }

    "JTSMultiPoint" should "be instantiable from a Seq of JTSPoint" in {
        val multiPointReference = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
        val pointsSeq = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val multiPointTest = JTSMultiPoint.fromSeq(pointsSeq)
        multiPointReference.equals(multiPointTest) shouldBe true
    }

    "JTSMultiPoint" should "return a Seq of MosaicPointJTS object when calling asSeq" in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[JTSMultiPoint]
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[JTSPoint])
        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: JTSPoint, b: JTSPoint) => a.equals(b) }
        results should contain only true
    }

    "JTSMultiPoint" should "not fail for empty Seq" in {
        val expected = JTSMultiPoint.fromWKT(
            "MULTIPOINT EMPTY"
        )
        val actual = JTSMultiPoint.fromSeq(Seq[JTSMultiPoint]())
        expected.equals(actual) shouldBe true
    }


    "JTSMultiPoint" should "return a Seq of JTSPoint object with the correct SRID when calling asSeq" in {
        val srid = 32632
        val multiPoint = JTSMultiPoint
            .fromWKT("MULTIPOINT (1 1, 2 2, 3 3)")
            .asInstanceOf[JTSMultiPoint]
        multiPoint.setSpatialReference(srid)
        val pointsSeqReference = Seq("POINT (1 1)", "POINT (2 2)", "POINT (3 3)")
            .map(JTSPoint.fromWKT)
            .map(_.asInstanceOf[JTSPoint])
        pointsSeqReference.foreach(_.setSpatialReference(srid))

        val pointSeqTest = multiPoint.asSeq.map(_.asInstanceOf[JTSPoint])
        pointSeqTest.map(_.getSpatialReference) should contain only srid

        val results = pointsSeqReference
            .zip(pointSeqTest)
            .map { case (a: JTSPoint, b: JTSPoint) => a.getSpatialReference == b.getSpatialReference }
        results should contain only true
    }

    "JTSMultiPoint" should "maintain SRID across operations" in {
        val srid = 32632
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT (1 1, 2 2, 3 3)").asInstanceOf[JTSMultiPoint]
        val anotherPoint = JTSPoint.fromWKT("POINT(1 1)").asInstanceOf[JTSPoint]
        val poly = JTSPolygon.fromWKT("POLYGON ((0 1,3 0,4 3,0 4,0 1))")

        multiPoint.setSpatialReference(srid)

        // MosaicGeometryJTS
        multiPoint.buffer(2d).getSpatialReference shouldBe srid
        multiPoint.convexHull.getSpatialReference shouldBe srid
        multiPoint.getCentroid.getSpatialReference shouldBe srid
        multiPoint.intersection(poly).getSpatialReference shouldBe srid
        multiPoint.rotate(45).getSpatialReference shouldBe srid
        multiPoint.scale(2d, 2d).getSpatialReference shouldBe srid
        multiPoint.simplify(0.001).getSpatialReference shouldBe srid
        multiPoint.translate(2d, 2d).getSpatialReference shouldBe srid
        multiPoint.union(anotherPoint).getSpatialReference shouldBe srid

        // MosaicMultiPoint
        multiPoint.asSeq.head.getSpatialReference shouldBe srid
        multiPoint.flatten.head.getSpatialReference shouldBe srid
        multiPoint.getShellPoints.head.head.getSpatialReference shouldBe srid

        // MosaicMultiPointJTS
        multiPoint.getBoundary.getSpatialReference shouldBe srid
        multiPoint.mapXY({ (x: Double, y: Double) => (x * 2, y / 2) }).getSpatialReference shouldBe srid
    }

    private val emptyLineString = JTSMultiPoint.fromWKT("LINESTRING EMPTY").asInstanceOf[JTSLineString]

    "JTSMultiPoint" should "perform an unconstrained Delauny tringulation" in {

        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT Z (2 1 0, 3 2 1, 1 3 3, 0 2 2)").asInstanceOf[JTSMultiPoint]
        val triangulated = multiPoint.triangulate(Seq(emptyLineString), 0.00, 0.01, TriangulationSplitPointTypeEnum.NONENCROACHING)
        JTSMultiPolygon.fromSeq(triangulated).toWKT shouldBe "MULTIPOLYGON Z(((0 2 2, 2 1 0, 1 3 3, 0 2 2)), ((1 3 3, 2 1 0, 3 2 1, 1 3 3)))"
    }

    "JTSMultiPoint" should "generate an equally spaced grid of points for use in elevation interpolation" in {
        val origin = JTSPoint.fromWKT("POINT (-0.5 -0.5)").asInstanceOf[JTSPoint]
        val grid = JTSMultiPoint.fromWKT("MULTIPOINT (0 0, 0 1, 0 2, 1 0, 1 1, 1 2, 2 0, 2 1, 2 2)").asInstanceOf[JTSMultiPoint]
        val generatedGrid = grid.pointGrid(origin, 3, 3, 1.0, 1.0)
        generatedGrid.toWKT shouldBe grid.toWKT
    }

    "JTSMultiPoint" should "perform elevation interpolation" in {
        val multiPoint = JTSMultiPoint.fromWKT("MULTIPOINT Z (2.5 1.5 0, 3.5 2.5 1, 1.5 3.5 3, 0.5 2.5 2)").asInstanceOf[JTSMultiPoint]
        val origin = JTSPoint.fromWKT("POINT (-0.5 -0.5)").asInstanceOf[JTSPoint]
        val gridPoints = multiPoint.pointGrid(origin, 5, 5, 1, 1).intersection(multiPoint.convexHull).asInstanceOf[JTSMultiPoint]
        val z = multiPoint.interpolateElevation(Seq(emptyLineString), gridPoints, 0.00, 0.01, TriangulationSplitPointTypeEnum.NONENCROACHING)
        z.toWKT shouldBe "MULTIPOINT Z((1 3 2.5), (2 2 0.8333333333333334), (2 3 2.1666666666666665), (3 2 0.5))"
        z.asSeq.map(_.getZ) shouldBe Seq(2.5, 0.8333333333333334, 2.1666666666666665, 0.5)
    }

}
