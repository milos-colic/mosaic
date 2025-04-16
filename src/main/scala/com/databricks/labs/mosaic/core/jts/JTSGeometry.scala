package com.databricks.labs.mosaic.core.jts

import com.databricks.labs.mosaic.core.crs.CRSBoundsProvider
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, InternalGeometry, TriangulationSplitPointTypeEnum}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.sql.catalyst.InternalRow
import org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER
import org.gdal.osr.{CoordinateTransformation, SpatialReference}
import org.locationtech.jts.algorithm.hull.ConcaveHull
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryCollection, GeometryFactory, Point, Polygon, Triangle}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io._
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}
import org.locationtech.jts.operation.buffer.{BufferOp, BufferParameters}
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier
import org.locationtech.proj4j.{CRSFactory, CoordinateTransformFactory, ProjCoordinate}

import java.util
import java.util.Locale
import scala.util.Try
import scala.collection.JavaConverters._

abstract class JTSGeometry(_g: Geometry) {

    def geom: Geometry = _g

    def getNumGeometries: Int = geom.getNumGeometries

    def getDimension: Int = getCoordinateSequence.getDimension

    private def getCoordinateSequence: CoordinateSequence = {
        CoordinateArraySequenceFactory.instance().create(geom.getCoordinates)
    }

    def compactGeometry: JTSGeometry = {
        val geometries = for (i <- 0 until getNumGeometries) yield geom.getGeometryN(i)
        val result = JTSGeometry.compactCollection(geometries, getSpatialReference)
        result.setSRID(geom.getSRID)
        JTSGeometry(result)
    }

    def translate(xd: Double, yd: Double): JTSGeometry = {
        val transformation = AffineTransformation.translationInstance(xd, yd)
        JTSGeometry(transformation.transform(geom))
    }

    def scale(xd: Double, yd: Double): JTSGeometry = {
        val transformation = AffineTransformation.scaleInstance(xd, yd)
        JTSGeometry(transformation.transform(geom))
    }

    def rotate(td: Double): JTSGeometry = {
        val transformation = AffineTransformation.rotationInstance(td)
        JTSGeometry(transformation.transform(geom))
    }

    def getCentroid: JTSPoint = {
        val centroid = geom.getCentroid
        centroid.setSRID(geom.getSRID)
        JTSPoint(centroid)
    }

    def getAnyPoint: JTSPoint = {
        // while this doesn't return the centroid but an arbitrary point via getCoordinate in JTS,
        // in like getCentroid this supports a Z coordinate.

        val coord = geom.getCoordinate
        val gf = new GeometryFactory()
        val point = gf.createPoint(coord)
        JTSPoint(point)
    }

    def isEmpty: Boolean = geom.isEmpty

    def boundary: JTSGeometry = JTSGeometry(geom.getBoundary)

    def envelope: JTSGeometry = JTSGeometry(geom.getEnvelope)

    def buffer(distance: Double): JTSGeometry = {
        buffer(distance, "")
    }

    def buffer(distance: Double, bufferStyleParameters: String): JTSGeometry = {

        val gBuf = new BufferOp(geom)

        if (bufferStyleParameters contains "=") {
            val params = bufferStyleParameters
                .split(" ")
                .map(_.split("="))
                .map { case Array(k, v) => (k, v) }
                .toMap

            if (params.contains("endcap")) {
                val capStyle = params.getOrElse("endcap", "")
                val capStyleConst = capStyle match {
                    case "round"  => BufferParameters.CAP_ROUND
                    case "flat"   => BufferParameters.CAP_FLAT
                    case "square" => BufferParameters.CAP_SQUARE
                    case _        => BufferParameters.CAP_ROUND
                }
                gBuf.setEndCapStyle(capStyleConst)
            }
            if (params.contains("quad_segs")) {
                val quadSegs = params.getOrElse("quad_segs", "8")
                gBuf.setQuadrantSegments(quadSegs.toInt)
            }
        }
        val buffered = gBuf.getResultGeometry(distance)
        buffered.setSRID(geom.getSRID)
        JTSGeometry(buffered)
    }

    def bufferCapStyle(distance: Double, capStyle: String): JTSGeometry = {
        val capStyleConst = capStyle match {
            case "round"  => BufferParameters.CAP_ROUND
            case "flat"   => BufferParameters.CAP_FLAT
            case "square" => BufferParameters.CAP_SQUARE
            case _        => BufferParameters.CAP_ROUND
        }
        val gBuf = new BufferOp(geom)
        gBuf.setEndCapStyle(capStyleConst)
        val buffered = gBuf.getResultGeometry(distance)
        buffered.setSRID(geom.getSRID)
        JTSGeometry(buffered)
    }

    def simplify(tolerance: Double = 1e-8): JTSGeometry = {
        val simplified = DouglasPeuckerSimplifier.simplify(geom, tolerance)
        simplified.setSRID(geom.getSRID)
        JTSGeometry(simplified)
    }

    def intersection(other: JTSGeometry): JTSGeometry = {
        val otherGeom = other.getGeom
        val intersection = this.geom.intersection(otherGeom)
        intersection.setSRID(geom.getSRID)
        if (intersection.getNumGeometries > 1) {
            val geometries = for (i <- 0 until intersection.getNumGeometries) yield intersection.getGeometryN(i)
            val result = JTSGeometry.compactCollection(geometries, getSpatialReference)
            result.setSRID(geom.getSRID)
            JTSGeometry(result)
        } else {
            intersection.setSRID(geom.getSRID)
            JTSGeometry(intersection)
        }
    }

    def intersects(other: JTSGeometry): Boolean = {
        val otherGeom = other.getGeom
        this.geom.intersects(otherGeom)
    }

    def difference(other: JTSGeometry): JTSGeometry = {
        val otherGeom = other.getGeom
        val leftType = GeometryTypeEnum.fromString(getGeometryType)
        val rightType = GeometryTypeEnum.fromString(other.getGeometryType)
        // Difference not supported for GeometryCollection in base APIs for JTS
        // If either of the geometries is a GeometryCollection, we need to
        // handle it differently, the logic is in the MosaicGeometryCollectionJTS
        val difference =
            if (leftType == GEOMETRYCOLLECTION) {
                this.asInstanceOf[JTSGeometryCollection].difference(other)
            } else if (rightType == GEOMETRYCOLLECTION) {
                other.asInstanceOf[JTSGeometryCollection].difference(this)
            } else {
                JTSGeometry(this.geom.difference(otherGeom))
            }
        difference.setSpatialReference(getSpatialReference)
        difference
    }

    def union(other: JTSGeometry): JTSGeometry = {
        val otherGeom = other.getGeom
        val leftType = GeometryTypeEnum.fromString(getGeometryType)
        val rightType = GeometryTypeEnum.fromString(other.getGeometryType)
        // Union not supported for GeometryCollection in base APIs for JTS
        // If either of the geometries is a GeometryCollection, we need to
        // handle it differently, the logic is in the MosaicGeometryCollectionJTS
        val union =
            if (leftType == GEOMETRYCOLLECTION) {
                this.asInstanceOf[JTSGeometryCollection].union(other)
            } else if (rightType == GEOMETRYCOLLECTION) {
                other.asInstanceOf[JTSGeometryCollection].union(this)
            } else {
                JTSGeometry(this.geom.union(otherGeom))
            }
        union.setSpatialReference(this.getSpatialReference)
        union
    }

    def contains(geom2: JTSGeometry): Boolean = geom.contains(geom2.getGeom)

    def within(geom2: JTSGeometry): Boolean = geom.within(geom2.getGeom)

    def getGeom: Geometry = geom

    def isValid: Boolean = geom.isValid

    def getGeometryType: String = geom.getGeometryType

    def getArea: Double = geom.getArea

    def equals(other: JTSGeometry): Boolean = {
        val otherGeom = other.getGeom
        this.geom.equalsExact(otherGeom)
    }

    override def equals(other: java.lang.Object): Boolean = {
        if (other == null) return false
        if (!other.isInstanceOf[JTSGeometry]) return false
        val otherGeom = other.asInstanceOf[JTSGeometry]
        this.geom.equals(otherGeom.geom)
    }

    def equalsTopo(other: JTSGeometry): Boolean = {
        val otherGeom = other.getGeom
        this.geom.equalsTopo(otherGeom)
    }

    override def hashCode: Int = geom.hashCode()

    def getLength: Double = geom.getLength

    def distance(geom2: JTSGeometry): Double = getGeom.distance(geom2.getGeom)

    def convexHull: JTSGeometry = {
        val convexHull = geom.convexHull()
        convexHull.setSRID(geom.getSRID)
        JTSGeometry(convexHull)
    }

    def concaveHull(lengthRatio: Double, allow_holes: Boolean = false): JTSGeometry = {
        val concaveHull = ConcaveHull.concaveHullByLengthRatio(geom, lengthRatio, allow_holes)
        concaveHull.setSRID(geom.getSRID)
        JTSGeometry(concaveHull)
    }

    def unaryUnion: JTSGeometry = {
        val unaryUnion = geom.union()
        unaryUnion.setSRID(geom.getSRID)
        JTSGeometry(unaryUnion)
    }

    def extent: (Double, Double, Double, Double) = {
        val env = envelope
        (
          env.minMaxCoord("X", "MIN"),
          env.minMaxCoord("Y", "MIN"),
          env.minMaxCoord("X", "MAX"),
          env.minMaxCoord("Y", "MAX")
        )
    }

    def minMaxCoord(dimension: String, func: String): Double = {
        val coordArray = this.getShellPoints.map(shell => {
            val unitArray = dimension.toUpperCase(Locale.ROOT) match {
                case "X" => shell.map(_.getX)
                case "Y" => shell.map(_.getY)
                case "Z" => shell.map(_.getZ)
            }
            func.toUpperCase(Locale.ROOT) match {
                case "MIN" => unitArray.min
                case "MAX" => unitArray.max
            }
        })
        func.toUpperCase(Locale.ROOT) match {
            case "MIN" => coordArray.min
            case "MAX" => coordArray.max
        }
    }

    def flatten: Seq[JTSGeometry]

    def getShellPoints: Seq[Seq[JTSPoint]]

    def getHolePoints: Seq[Seq[Seq[JTSPoint]]]

    def getHoles: Seq[Seq[JTSLineString]]

    def getShells: Seq[JTSLineString]

    def osrTransformCRS(srcSR: SpatialReference, destSR: SpatialReference): JTSGeometry = {
        if (srcSR.IsSame(destSR) == 1) return this

        val transform = new CoordinateTransformation(srcSR, destSR)

        def mapper(x: Double, y: Double): (Double, Double) = {
            val p = transform.TransformPoint(x, y).toSeq.take(2)
            (p(0), p(1))
        }

        val srID = Try(destSR.AutoIdentifyEPSG()).getOrElse(4326)

        val mosaicGeometry = mapXY(mapper)
        mosaicGeometry.setSpatialReference(srID)
        mosaicGeometry

    }

    def transformCRSXY(sridTo: Int, sridFrom: Option[Int]): JTSGeometry = {

        val crsFactory = new CRSFactory
        val crsFrom = crsFactory.createFromName(f"epsg:${sridFrom.getOrElse(getSpatialReference)}")
        val crsTo = crsFactory.createFromName(f"epsg:$sridTo")

        val ctFactory = new CoordinateTransformFactory
        val trans = ctFactory.createTransform(crsFrom, crsTo)

        val pIn = new ProjCoordinate
        val pOut = new ProjCoordinate

        def mapper(x: Double, y: Double): (Double, Double) = {
            pIn.setValue(x, y)
            trans.transform(pIn, pOut)
            (pOut.x, pOut.y)
        }
        val mosaicGeometry = mapXY(mapper)
        mosaicGeometry.setSpatialReference(sridTo)
        mosaicGeometry
    }

    def getSpatialReferenceOSR: SpatialReference = {
        val srID = getSpatialReference
        if (srID == 0) {
            null
        } else {
            val geomCRS = new SpatialReference()
            geomCRS.ImportFromEPSG(srID)
            geomCRS.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER)
            geomCRS
        }
    }

    def hasValidCoords(crsBoundsProvider: CRSBoundsProvider, crsCode: String, which: String): Boolean = {
        val crsCodeIn = crsCode.split(":")
        val crsBounds = which.toLowerCase(Locale.ROOT) match {
            case "bounds"             => crsBoundsProvider.bounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case "reprojected_bounds" => crsBoundsProvider.reprojectedBounds(crsCodeIn(0), crsCodeIn(1).toInt)
            case _                    => throw new Error("Only boundary and reprojected_boundary supported for which argument.")
        }
        (Seq(getShellPoints) ++ getHolePoints).flatten.flatten.forall(point =>
            crsBounds.lowerLeft.getX <= point.getX && point.getX <= crsBounds.upperRight.getX &&
            crsBounds.lowerLeft.getY <= point.getY && point.getY <= crsBounds.upperRight.getY
        )
    }

    def mapXY(f: (Double, Double) => (Double, Double)): JTSGeometry

    def toWKT: String = new WKTWriter(getDimension).write(geom)

    def toWKT(coordDims: Int): String = new WKTWriter(coordDims).write(geom)

    def toJSON: String = new GeoJsonWriter().write(geom)

    def toHEX: String = WKBWriter.toHex(toWKB)

    def toWKB: Array[Byte] = new WKBWriter(getDimension).write(geom)

    def toWKB(coordDims: Int): Array[Byte] = new WKBWriter(coordDims).write(geom)

    def numPoints: Int = geom.getNumPoints

    def getSpatialReference: Int = geom.getSRID

    def setSpatialReference(srid: Int): Unit = geom.setSRID(srid)

    def transformCRSXY(sridTo: Int): JTSGeometry = {
        transformCRSXY(sridTo, Some(getSpatialReference))
    }

    def toInternal: InternalGeometry

    def getBoundary: JTSGeometry

    def asSeq: Seq[JTSGeometry]

    def triangulate(
        breaklines: Seq[JTSLineString],
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: TriangulationSplitPointTypeEnum.Value
    ): Seq[JTSPolygon] = {
        // Treat MULTI geometries as a collection of individual points => multipoint
        val vertices = getShellPoints.flatten ++ getHolePoints.flatten.flatten
        // Make sure vertices are unique, as geometry collections can have overlapping geometries
        // and there may be duplicate points in the collection
        val asMultiPoint = JTSMultiPoint.fromSeq(vertices.distinct)
        asMultiPoint.triangulate(breaklines, mergeTolerance, snapTolerance, splitPointFinder)
    }

    def pointGrid(origin: JTSPoint, xCells: Int, yCells: Int, xSize: Double, ySize: Double): JTSMultiPoint = {
        val gridPoints = for (i <- 0 until xCells; j <- 0 until yCells) yield {
            val x = origin.getX + i * xSize + xSize / 2
            val y = origin.getY + j * ySize + ySize / 2
            val gridPoint = JTSPoint(geom.getFactory.createPoint(new Coordinate(x, y)))
            gridPoint.setSpatialReference(getSpatialReference)
            gridPoint
        }
        JTSMultiPoint.fromSeq(gridPoints)
    }

    def interpolateElevation(
        breaklines: Seq[JTSLineString],
        gridPoints: JTSMultiPoint,
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: TriangulationSplitPointTypeEnum.Value
    ): JTSMultiPoint = {
        val triangles = triangulate(breaklines, mergeTolerance, snapTolerance, splitPointFinder)
            .asInstanceOf[Seq[JTSPolygon]]

        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getGeom.getEnvelopeInternal, p.getGeom))

        val result = gridPoints.asSeq
            .map(p => {
                val point = p.getGeom.asInstanceOf[Point]
                point -> tree
                    .query(p.getGeom.getEnvelopeInternal)
                    .asScala
                    .map(_.asInstanceOf[Polygon])
                    .find(_.intersects(point))
            })
            .toMap
            .collect({ case (pt, Some(ply)) => pt -> ply })
            .map({ case (point: Point, poly: Polygon) =>
                val polyCoords = poly.getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                if (z.isNaN) { throw new Exception("Interpolated Z value is NaN") }
                val interpolatedPoint = JTSPoint(point.getFactory.createPoint(new Coordinate(point.getX, point.getY, z)))
                interpolatedPoint.setSpatialReference(getSpatialReference)
                interpolatedPoint
            })
            .toSeq
        JTSMultiPoint.fromSeq(result)
    }

}

object JTSGeometry {

    @transient private val kryo = new Kryo()
    kryo.register(classOf[JTSGeometry])

    def fromWKT(wkt: String): JTSGeometry = JTSGeometry(new WKTReader().read(wkt))

    // noinspection DuplicatedCode
    def compactCollection(geometries: Seq[Geometry], srid: Int): Geometry = {
        def appendGeometries(geometries: util.ArrayList[Geometry], toAppend: Seq[Geometry]): Unit = {
            if (toAppend.length == 1 && !toAppend.head.isEmpty) {
                geometries.add(toAppend.head)
            } else if (toAppend.length > 1) {
                val compacted = toAppend.reduce(_ union _)
                if (!compacted.isEmpty) {
                    geometries.add(compacted)
                }
            }
        }

        val withType = geometries.map(g => GeometryTypeEnum.fromString(g.getGeometryType) -> g).flatMap {
            case (GEOMETRYCOLLECTION, g) =>
                val collection = g.asInstanceOf[GeometryCollection]
                for (i <- 0 until collection.getNumGeometries)
                    yield GeometryTypeEnum.fromString(collection.getGeometryN(i).getGeometryType) -> collection.getGeometryN(i)
            case (gType, g)              => Seq(gType -> g)
        }
        val points = withType.filter(g => g._1 == POINT || g._1 == MULTIPOINT).map(_._2)
        val polygons = withType.filter(g => g._1 == POLYGON || g._1 == MULTIPOLYGON).map(_._2)
        val lines = withType.filter(g => g._1 == LINESTRING || g._1 == MULTILINESTRING).map(_._2)
        val geomArray = new util.ArrayList[Geometry]()

        appendGeometries(geomArray, points)
        appendGeometries(geomArray, lines)
        appendGeometries(geomArray, polygons)

        val gf = new GeometryFactory()
        val geom = gf.buildGeometry(geomArray)
        val result =
            if (geom.getNumGeometries == 1) {
                geom.getGeometryN(0)
            } else {
                geom
            }
        result.setSRID(srid)
        result
    }

    def apply(geom: Geometry): JTSGeometry = {
        GeometryTypeEnum.fromString(geom.getGeometryType) match {
            case POINT              => JTSPoint(geom)
            case MULTIPOINT         => JTSMultiPoint(geom)
            case POLYGON            => JTSPolygon(geom)
            case MULTIPOLYGON       => JTSMultiPolygon(geom)
            case LINESTRING         => JTSLineString(geom)
            case MULTILINESTRING    => JTSMultiLineString(geom)
            case LINEARRING         => JTSLineString(geom)
            case GEOMETRYCOLLECTION => JTSGeometryCollection(geom)
        }
    }

    def fromHEX(hex: String): JTSGeometry = {
        val bytes = WKBReader.hexToBytes(hex)
        fromWKB(bytes)
    }

    def fromWKB(wkb: Array[Byte]): JTSGeometry = JTSGeometry(new WKBReader().read(wkb))

    def fromJSON(geoJson: String): JTSGeometry = JTSGeometry(new GeoJsonReader().read(geoJson))

    def fromSeq[T <: JTSGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value): JTSGeometry = {
        reader(geomType.id).fromSeq(geomSeq, geomType)
    }

    def fromInternal(row: InternalRow): JTSGeometry = {
        val typeId = row.getInt(0)
        reader(typeId).fromInternal(row)
    }

    def reader(geomTypeId: Int): GeometryReader =
        GeometryTypeEnum.fromId(geomTypeId) match {
            case POINT              => JTSPoint
            case MULTIPOINT         => JTSMultiPoint
            case POLYGON            => JTSPolygon
            case MULTIPOLYGON       => JTSMultiPolygon
            case LINESTRING         => JTSLineString
            case MULTILINESTRING    => JTSMultiLineString
            case GEOMETRYCOLLECTION => JTSGeometryCollection
        }

}
