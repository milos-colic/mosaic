package com.databricks.labs.mosaic.core

import com.databricks.labs.mosaic.core.index.IndexSystem
import com.databricks.labs.mosaic.core.jts._
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum._
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, MosaicChip}

import scala.annotation.tailrec

/**
  * Single abstracted logic for mosaic fill via [[IndexSystem]]. [[IndexSystem]]
  * is in charge of implementing the individual steps of the logic.
  */
object Mosaic {

    def getChips(
                    geometry: JTSGeometry,
                    resolution: Int,
                    keepCoreGeom: Boolean,
                    indexSystem: IndexSystem
    ): Seq[MosaicChip] = {
        GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case POINT           => pointChip(geometry, resolution, keepCoreGeom, indexSystem)
            case MULTIPOINT      => multiPointChips(geometry, resolution, keepCoreGeom, indexSystem)
            case LINESTRING      => lineFill(geometry, resolution, indexSystem)
            case MULTILINESTRING => lineFill(geometry, resolution, indexSystem)
            case _               => mosaicFill(geometry, resolution, keepCoreGeom, indexSystem)
        }
    }

    def multiPointChips(
        geometry: JTSGeometry,
        resolution: Int,
        keepCoreGeom: Boolean,
        indexSystem: IndexSystem
    ): Seq[MosaicChip] = {
        val points = geometry.asInstanceOf[JTSMultiPoint].asSeq
        points.flatMap(point => pointChip(point, resolution, keepCoreGeom, indexSystem))
    }

    def pointChip(
        geometry: JTSGeometry,
        resolution: Int,
        keepCoreGeom: Boolean,
        indexSystem: IndexSystem
    ): Seq[MosaicChip] = {
        val point = geometry.asInstanceOf[JTSPoint]
        val chipGeom = if (keepCoreGeom) point else null
        val cellId = indexSystem.pointToIndex(point.getX, point.getY, resolution)
        val chip = MosaicChip(isCore = false, Left(cellId), chipGeom)
        Seq(chip.formatCellId(indexSystem))
    }

    def mosaicFill(
        geometry: JTSGeometry,
        resolution: Int,
        keepCoreGeom: Boolean,
        indexSystem: IndexSystem
    ): Seq[MosaicChip] = {

        val radius = indexSystem.getBufferRadius(geometry, resolution)
        // do not modify the radius
        val carvedGeometry = geometry.buffer(-radius)


        // add 1% to the radius to ensure union of carved and border geometries does not have holes inside the original geometry areas
        val borderGeometry =
            if (carvedGeometry.isEmpty) {
                geometry
                    .buffer(radius * 1.01)
                    .simplify(0.01 * radius)
            } else {
                geometry.boundary
                    .buffer(radius * 1.01)
                    .simplify(0.01 * radius)
            }

        // check that the resulting geometry is within the bounds of
        // the coordinate system (otherwise behaviour will be unpredictable)
        val originalGeometryConstrained = indexSystem.alignToGrid(geometry)
        val carvedGeometryConstrained = indexSystem.alignToGrid(carvedGeometry)
        val borderGeometryConstrained = indexSystem.alignToGrid(borderGeometry)

        val coreIndices = indexSystem.polyfill(carvedGeometryConstrained, resolution)
        val borderIndices = indexSystem.polyfill(borderGeometryConstrained, resolution).diff(coreIndices)

        val coreChips = indexSystem.getCoreChips(coreIndices, keepCoreGeom)
        val borderChips = indexSystem.getBorderChips(originalGeometryConstrained, borderIndices, keepCoreGeom)

        coreChips ++ borderChips
    }

    def lineFill(geometry: JTSGeometry, resolution: Int, indexSystem: IndexSystem): Seq[MosaicChip] = {
        GeometryTypeEnum.fromString(geometry.getGeometryType) match {
            case LINESTRING      => lineDecompose(geometry.asInstanceOf[JTSLineString], resolution, indexSystem)
            case MULTILINESTRING =>
                val multiLine = geometry.asInstanceOf[JTSMultiLineString]
                multiLine.flatten.flatMap(line => lineDecompose(line.asInstanceOf[JTSLineString], resolution, indexSystem))
            case gt              => throw new Error(s"$gt not supported for line fill/decompose operation.")
        }
    }

    /**
      * @param geometry
      *   Geometry to get k ring cells for.
      * @param resolution
      *   Resolution of the cells to get.
      * @param indexSystem
      *   Index system to use.
      * @return
      *   A set of k ring cells for the geometry.
      */
    def geometryKRing(geometry: JTSGeometry, resolution: Int, k: Int, indexSystem: IndexSystem): Set[Long] = {
        val (coreCells, borderCells) = getCellSets(geometry, resolution, indexSystem)
        val borderKRing = borderCells.flatMap(indexSystem.kRing(_, k))
        val kRing = coreCells ++ borderKRing
        kRing
    }

    /**
      * @param geometry
      *   Geometry to get k loop around
      * @param resolution
      *   Resolution of the cells
      * @param indexSystem
      *   Index system to use
      * @return
      *   Set of cells that form a k loop around geometry
      */
    def geometryKLoop(geometry: JTSGeometry, resolution: Int, k: Int, indexSystem: IndexSystem): Set[Long] = {
        val n: Int = k - 1
        // This would be much more efficient if we could use the
        // pre-computed tessellation of the geometry for repeated calls.
        val (coreCells, borderCells) = getCellSets(geometry, resolution, indexSystem)

        // We use nRing as naming for kRing where k = n
        val borderNRing = borderCells.flatMap(indexSystem.kRing(_, n))
        val nRing = coreCells ++ borderNRing

        val borderKLoop = borderCells.flatMap(indexSystem.kLoop(_, k))

        val kLoop = borderKLoop -- nRing
        kLoop
    }

    private def lineDecompose(
        line: JTSLineString,
        resolution: Int,
        indexSystem: IndexSystem
    ): Seq[MosaicChip] = {
        val start = line.getShells.head.asSeq.head
        val startIndex = indexSystem.pointToIndex(start.getX, start.getY, resolution)

        @tailrec
        def traverseLine(
            line: JTSLineString,
            queue: Seq[Long],
            traversed: Set[Long],
            chips: Seq[MosaicChip]
        ): Seq[MosaicChip] = {
            val newTraversed = traversed ++ queue
            val (newQueue, newChips) = queue.foldLeft(
              (Seq.empty[Long], chips)
            )((accumulator: (Seq[Long], Seq[MosaicChip]), current: Long) => {
                val indexGeom = indexSystem.indexToGeometry(current)
                val lineSegment = line.intersection(indexGeom)
                if (!lineSegment.isEmpty) {
                    val chip = MosaicChip(isCore = false, Left(current), lineSegment)
                    val kRing = indexSystem.kRing(current, 1)
                    
                    // Ignore already processed chips and those which are already in the
                    // queue to be processed
                    val toQueue = kRing.filterNot((newTraversed ++ accumulator._1).contains)
                    (accumulator._1 ++ toQueue, accumulator._2 ++ Seq(chip))
                } else if (newTraversed.size == 1) {
                    // The line segment intersection was empty, but we only intersected the first point
                    // with a single cell.
                    // We need to run an intersection with a first ring because the starting point might be laying
                    // exactly on the cell boundary.
                    val kRing = indexSystem.kRing(current, 1)
                    val toQueue = kRing.filterNot(newTraversed.contains)
                    (toQueue, accumulator._2)
                } else {
                    accumulator
                }
            })
            if (newQueue.isEmpty) {
                newChips
            } else {
                traverseLine(line, newQueue, newTraversed, newChips)
            }
        }

        val result = traverseLine(line, Seq(startIndex), Set.empty[Long], Seq.empty[MosaicChip])
        result
    }

    /**
      * Returns core cells and border cells as a sets of Longs. The
      * implementation currently depends on [[getChips()]] method.
      *
      * @param geometry
      *   Geometry to fill with cells.
      * @param resolution
      *   Resolution of the cells.
      * @param indexSystem
      *   Index system to use.
      * @return
      *   Tuple of core cells and border cells.
      */
    private def getCellSets(
        geometry: JTSGeometry,
        resolution: Int,
        indexSystem: IndexSystem
    ): (Set[Long], Set[Long]) = {
        val chips = Mosaic.getChips(geometry, resolution, keepCoreGeom = false, indexSystem)
        val (coreChips, borderChips) = chips.partition(_.isCore)

        val coreCells = coreChips.map(_.cellIdAsLong(indexSystem)).toSet
        val borderCells = borderChips.map(_.cellIdAsLong(indexSystem)).toSet
        (coreCells, borderCells)
    }

}
