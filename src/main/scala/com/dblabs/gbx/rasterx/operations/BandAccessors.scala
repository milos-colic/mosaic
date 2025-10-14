package com.dblabs.gbx.rasterx.operations

import org.gdal.gdal.Band
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.DictionaryHasAsScala
import scala.util.Try

object BandAccessors {

    def getMetadata(band: Band): Map[String, String] = {
        if (Option(band).isEmpty) Map.empty
        else {
            Option(band.GetMetadata_Dict())
                .map(_.asScala.toMap.asInstanceOf[Map[String, String]])
                .getOrElse(Map.empty[String, String])
        }
    }

    def getMinMax(band: Band): (Double, Double) = {
        val minmax = Array.ofDim[Double](2)
        if (Option(band).isEmpty) (Double.NaN, Double.NaN)
        else {
            band.ComputeRasterMinMax(minmax, 0)
            (minmax(0), minmax(1))
        }
    }

    def getNoDataValue(band: Band): Double = {
        if (Option(band).isEmpty) Double.NaN
        else {
            val noDataVal = Array.fill[java.lang.Double](1)(0)
            band.GetNoDataValue(noDataVal)
            if (noDataVal(0) == null || noDataVal(0).isNaN) Double.NaN
            else {
                val noDataValue = noDataVal(0).doubleValue()
                if (noDataValue == Double.NaN) Double.NaN
                else noDataValue
            }
        }
    }

    def dataTypeHuman(band: Band): String =
        Try(band.getDataType).getOrElse(0) match {
            case gdalconstConstants.GDT_Byte     => "Byte"
            case gdalconstConstants.GDT_UInt16   => "UInt16"
            case gdalconstConstants.GDT_Int16    => "Int16"
            case gdalconstConstants.GDT_UInt32   => "UInt32"
            case gdalconstConstants.GDT_Int32    => "Int32"
            case gdalconstConstants.GDT_Float32  => "Float32"
            case gdalconstConstants.GDT_Float64  => "Float64"
            case gdalconstConstants.GDT_CInt16   => "ComplexInt16"
            case gdalconstConstants.GDT_CInt32   => "ComplexInt32"
            case gdalconstConstants.GDT_CFloat32 => "ComplexFloat32"
            case gdalconstConstants.GDT_CFloat64 => "ComplexFloat64"
            case _                               => "Unknown"
        }

    def isEmpty(band: Band): Boolean = {
        val flags = band.GetMaskFlags()
        if ((flags & gdalconstConstants.GMF_ALL_VALID) != 0) return false
        val mask = band.GetMaskBand()
        if (Option(mask).isEmpty) return true

        val w = band.GetXSize()
        val h = band.GetYSize()
        val bW = mask.GetBlockXSize()
        val bH = mask.GetBlockYSize()
        
        // Use optimal buffer size for better performance
        val optimalBufferSize = getOptimalBufferSize(bW * bH)
        val buffer = java.nio.ByteBuffer.allocateDirect(optimalBufferSize)

        var y = 0
        while (y < h) {
            val rh = math.min(bH, h - y)
            var x = 0
            while (x < w) {
                val rw = math.min(bW, w - x)
                buffer.clear()
                buffer.limit(rw * rh)
                mask.ReadRaster_Direct(x, y, rw, rh, buffer)
                var i = 0; val lim = rw * rh
                while (i < lim) { if ((buffer.get(i) & 0xff) != 0) return false; i += 1 }
                x += bW
            }
            y += bH
        }
        true
    }

    /**
     * Calculates optimal buffer size for GDAL operations to reduce memory allocation overhead.
     * Uses power-of-2 sizing with minimum thresholds for better cache performance.
     */
    def getOptimalBufferSize(requiredSize: Int): Int = {
        val minSize = 4096  // 4KB minimum for better I/O performance
        if (requiredSize <= minSize) minSize
        else {
            // Round up to next power of 2, with reasonable maximum
            val maxSize = 64 * 1024 * 1024  // 64MB maximum
            val nextPowerOf2 = Integer.highestOneBit(requiredSize - 1) << 1
            math.min(nextPowerOf2, maxSize)
        }
    }

    /**
     * Efficiently reads multiple bands from the same raster region to reduce JNI overhead.
     * This is particularly useful when multiple bands need to be processed together.
     */
    def readMultipleBands(
        bands: Array[Band], 
        xOffset: Int, 
        yOffset: Int, 
        width: Int, 
        height: Int
    ): Array[Array[Double]] = {
        val pixelCount = width * height
        val result = Array.ofDim[Double](bands.length, pixelCount)
        
        // Read all bands in sequence with pre-allocated buffers
        var i = 0
        while (i < bands.length) {
            bands(i).ReadRaster(xOffset, yOffset, width, height, result(i))
            i += 1
        }
        
        result
    }

}
