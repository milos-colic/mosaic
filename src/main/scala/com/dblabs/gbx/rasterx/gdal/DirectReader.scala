package com.dblabs.gbx.rasterx.gdal

import com.dblabs.gbx.rasterx.operations.BandAccessors
import org.gdal.gdal.Band

final class DirectReader(initialCapacity: Int = 0) {

    private var dataBuf = new Array[Double](initialCapacity)
    private var maskBuf = new Array[Byte](initialCapacity)
    private var outputBuf = Array.ofDim[Double](0, 0)
    
    // Minimum buffer capacity to avoid frequent reallocations for small reads
    private val MIN_BUFFER_SIZE = 1024
    
    /**
     * Calculates optimal buffer size with exponential growth strategy
     * to reduce memory allocations during pixel reading operations.
     */
    private def optimalBufferSize(requiredSize: Int): Int = {
        if (requiredSize <= MIN_BUFFER_SIZE) MIN_BUFFER_SIZE
        else {
            // Find next power of 2 that's >= requiredSize, capped at reasonable limit
            val maxSize = 16 * 1024 * 1024 // 16MB limit for single buffer
            math.min(Integer.highestOneBit(requiredSize - 1) << 1, maxSize)
        }
    }

    def readWindow(
        band: Band,
        window: (Int, Int, Int, Int)
    ): Array[Array[Double]] = {
        val noData = BandAccessors.getNoDataValue(band)
        val (x0, y0, x1, y1) = window
        val xs = math.max(0, math.min(x0, x1))
        val ys = math.max(0, math.min(y0, y1))
        val w = math.abs(x1 - x0)
        val h = math.abs(y1 - y0)
        if (w <= 0 || h <= 0) return null

        val len = w * h
        if (dataBuf.length < len) {
            val optimalSize = optimalBufferSize(len)
            dataBuf = new Array[Double](optimalSize)
            maskBuf = new Array[Byte](optimalSize)
        }

        // read raw
        band.ReadRaster(xs, ys, w, h, dataBuf)
        band.GetMaskBand().ReadRaster(xs, ys, w, h, maskBuf)

        // track if we ever see a non-noData pixel
        var hasData = false
        var i = 0
        while (i < len) {
            if (maskBuf(i) == 0) {
                dataBuf(i) = noData
            } else if (dataBuf(i) != noData) {
                hasData = true
            }
            i += 1
        }

        if (!hasData) {
            // everything was masked or equal to noData
            return null
        }

        // prepare 2D buffer
        if (outputBuf.length != h || (h > 0 && outputBuf(0).length != w)) {
            outputBuf = Array.fill(h, w)(noData)
        }

        // bulk-copy rows
        var row = 0
        while (row < h) {
            System.arraycopy(dataBuf, row * w, outputBuf(row), 0, w)
            row += 1
        }

        outputBuf
    }

}
