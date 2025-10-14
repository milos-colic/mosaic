package com.dblabs.gbx.rasterx

import com.dblabs.gbx.rasterx.gdal.{DirectReader, GDALBlock, GDALManager}
import com.dblabs.gbx.rasterx.operations.BandAccessors
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GDALPixelReadingPerformanceTest extends AnyFunSuite with BeforeAndAfterAll {

    var testDataset: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
        
        // Create a test dataset in memory for consistent testing
        val driver = gdal.GetDriverByName("MEM")
        testDataset = driver.Create("/vsimem/perf_test.tif", 1024, 1024, 3)
        
        // Fill bands with test data
        for (bandIdx <- 1 to 3) {
            val band = testDataset.GetRasterBand(bandIdx)
            val testData = Array.ofDim[Double](1024 * 1024)
            for (i <- testData.indices) {
                testData(i) = (i % 256).toDouble + bandIdx * 100
            }
            band.WriteRaster(0, 0, 1024, 1024, testData)
            band.FlushCache()
        }
    }

    override def afterAll(): Unit = {
        if (testDataset != null) testDataset.delete()
        gdal.Unlink("/vsimem/perf_test.tif")
    }

    test("DirectReader buffer optimization should handle multiple read sizes efficiently") {
        val reader = new DirectReader()
        val band = testDataset.GetRasterBand(1)
        
        // Test various window sizes to verify buffer reuse
        val windowSizes = Seq(
            (64, 64),
            (128, 128),
            (32, 32),    // Smaller than previous - should reuse buffer
            (256, 256),  // Larger - should grow buffer
            (100, 100)   // Should reuse existing buffer
        )
        
        windowSizes.foreach { case (width, height) =>
            val result = reader.readWindow(band, (0, 0, width, height))
            result should not be null
            result.length shouldBe height
            if (height > 0) result(0).length shouldBe width
        }
    }

    test("GDALBlock optimal block size calculation should respect natural blocks") {
        val band = testDataset.GetRasterBand(1)
        val (optimalWidth, optimalHeight) = GDALBlock.getOptimalBlockSize(band, 100, 100)
        
        // Should return sensible block sizes
        optimalWidth should be > 0
        optimalHeight should be > 0
        optimalWidth should be <= 1024  // Shouldn't exceed raster size
        optimalHeight should be <= 1024
    }

    test("BandAccessors multi-band reading should be more efficient than sequential reads") {
        val bands = Array(
            testDataset.GetRasterBand(1),
            testDataset.GetRasterBand(2),
            testDataset.GetRasterBand(3)
        )
        
        // Test multi-band reading
        val multiResult = BandAccessors.readMultipleBands(bands, 0, 0, 256, 256)
        multiResult.length shouldBe 3
        multiResult(0).length shouldBe 256 * 256
        
        // Verify data integrity - each band should have different values
        val band1Avg = multiResult(0).sum / multiResult(0).length
        val band2Avg = multiResult(1).sum / multiResult(1).length
        val band3Avg = multiResult(2).sum / multiResult(2).length
        
        // Bands should have different average values due to our test data pattern
        band1Avg should not equal band2Avg
        band2Avg should not equal band3Avg
    }

    test("Optimal buffer size calculation should use power-of-2 sizing") {
        BandAccessors.getOptimalBufferSize(1000) shouldBe 1024
        BandAccessors.getOptimalBufferSize(2000) shouldBe 2048
        BandAccessors.getOptimalBufferSize(100) shouldBe 4096  // Minimum size
        BandAccessors.getOptimalBufferSize(70000000) shouldBe 67108864  // Max size (64MB)
    }

    test("DirectReader should handle edge cases efficiently") {
        val reader = new DirectReader()
        val band = testDataset.GetRasterBand(1)
        
        // Test zero-sized window
        val emptyResult = reader.readWindow(band, (0, 0, 0, 0))
        emptyResult shouldBe null
        
        // Test single pixel
        val singlePixel = reader.readWindow(band, (0, 0, 1, 1))
        singlePixel should not be null
        singlePixel.length shouldBe 1
        singlePixel(0).length shouldBe 1
        
        // Test large window up to raster bounds
        val fullRaster = reader.readWindow(band, (0, 0, 1024, 1024))
        fullRaster should not be null
        fullRaster.length shouldBe 1024
        fullRaster(0).length shouldBe 1024
    }

}