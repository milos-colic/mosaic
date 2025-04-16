package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.geometry.triangulation.JTSConformingDelaunayTriangulationBuilder
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.{MULTIPOINT, POINT}
import com.databricks.labs.mosaic.core.types.model._
import org.apache.spark.sql.catalyst.InternalRow
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine

import scala.collection.JavaConverters._

class JTSMultiPoint(multiPoint: MultiPoint) extends JTSGeometry(multiPoint) {

    // noinspection DuplicatedCode
    def toInternal: InternalGeometry = {
        val points = asSeq.map(_.coord).map(InternalCoord(_))
        new InternalGeometry(MULTIPOINT.id, getSpatialReference, Array(points.toArray), Array(Array(Array())))
    }

    def asSeq: Seq[JTSPoint] = {
        for (i <- 0 until multiPoint.getNumPoints) yield {
            val geom = multiPoint.getGeometryN(i)
            geom.setSRID(multiPoint.getSRID)
            JTSPoint(geom)
        }
    }

    def getBoundary: JTSGeometry = {
        val boundary = multiPoint.getBoundary
        boundary.setSRID(multiPoint.getSRID)
        JTSGeometry(boundary)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry = {
        JTSMultiPoint.fromSeq(asSeq.map(_.mapXY(f).asInstanceOf[JTSPoint]))
    }

    override def getHoles: Seq[Seq[JTSLineString]] = Nil

    override def flatten: Seq[JTSGeometry] = asSeq

    override def getHolePoints: Seq[Seq[Seq[JTSPoint]]] = Nil

    override def getShellPoints: Seq[Seq[JTSPoint]] = Seq(asSeq)

    override def triangulate(breaklines: Seq[JTSLineString], mergeTolerance: Double, snapTolerance: Double, splitPointFinder: TriangulationSplitPointTypeEnum.Value): Seq[JTSPolygon] = {

        val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        if (breaklines.nonEmpty) {
            triangulator.setConstraints(JTSMultiLineString.fromSeq(breaklines).getGeom)
        }

        triangulator.setTolerance(mergeTolerance)

        val trianglesGeomCollection = triangulator.getTriangles
        val trianglePolygons = PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Polygon])

        val postProcessedTrianglePolygons = postProcessTriangulation(trianglePolygons, JTSMultiLineString.fromSeq(breaklines).getGeom, snapTolerance)
        postProcessedTrianglePolygons.map(JTSPolygon(_))
    }

    /** Update Z values of the triangle vertices that have NaN Z values by interpolating from the constraint lines
     *
     * @param trianglePolygons: Sequence of triangles, output from the triangulation method
     * @param constraintLineGeom: Geometry containing the constraint lines
     * @param tolerance: Tolerance value for the triangulation, used to buffer points and match to constraint lines
     * @return Sequence of triangles with updated Z values
     * */
    private def postProcessTriangulation(trianglePolygons: Seq[Polygon], constraintLineGeom: Geometry, tolerance: Double): Seq[Polygon] = {
        val geomFact = constraintLineGeom.getFactory

        val constraintLines =
            LinearComponentExtracter.getLines(constraintLineGeom)
                .iterator().asScala.toSeq
                .map(_.asInstanceOf[LineString])

        val constraintLinesTree = new STRtree(4)
        constraintLines.foreach(l => constraintLinesTree.insert(l.getEnvelopeInternal, l))

        trianglePolygons.map(
            t => {
                val coords = t.getCoordinates.map(
                    c => {
                        /*
                        * overwrite the z values for every coordinate lying
                        * within a fraction of the value of `tolerance`.
                        */
                        val coordPoint = geomFact.createPoint(c)
                        val originatingLineString = constraintLinesTree.query(new Envelope(c))
                            .iterator().asScala.toSeq
                            .map(_.asInstanceOf[LineString])
                            .find(l => l.intersects(coordPoint.buffer(tolerance)))
                        originatingLineString match {
                            case Some(l) =>
                                val indexedLine = new LengthIndexedLine(l)
                                val index = indexedLine.indexOf(c)
                                indexedLine.extractPoint(index)
                            case None => c
                        }
                    }
                )
                geomFact.createPolygon(coords)
            }
        )
    }

    override def getShells: Seq[JTSLineString] = Nil
}

object JTSMultiPoint extends GeometryReader {

    // noinspection ZeroIndexToHead
    override def fromInternal(row: InternalRow): JTSMultiPoint = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        require(internalGeom.typeId == MULTIPOINT.id)

        val points = internalGeom.boundaries.head.map(p => gf.createPoint(p.toCoordinate))
        val multiPoint = gf.createMultiPoint(points)
        multiPoint.setSRID(internalGeom.srid)
        new JTSMultiPoint(multiPoint)
    }

    override def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = MULTIPOINT): JTSMultiPoint = {
        val gf = new GeometryFactory()
        if (geomSeq.isEmpty) {
            // For empty sequence return an empty geometry with default Spatial Reference
            return JTSMultiPoint(gf.createMultiPoint())
        }
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoints = geomSeq.map(_.asInstanceOf[JTSPoint])
                gf.createMultiPoint(extractedPoints.map(_.getGeom.asInstanceOf[Point]).toArray)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        JTSMultiPoint(newGeom)
    }

    def apply(geom: Geometry): JTSMultiPoint = new JTSMultiPoint(geom.asInstanceOf[MultiPoint])

    override def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry.fromWKB(wkb)

    override def fromWKT(wkt: String): JTSGeometry = JTSGeometry.fromWKT(wkt)

    override def fromJSON(geoJson: String): JTSGeometry = JTSGeometry.fromJSON(geoJson)

    override def fromHEX(hex: String): JTSGeometry = JTSGeometry.fromHEX(hex)

}
