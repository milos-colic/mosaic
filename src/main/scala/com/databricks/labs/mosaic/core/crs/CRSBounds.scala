package com.databricks.labs.mosaic.core.crs

import com.databricks.labs.mosaic.core.jts.{JTS, JTSPoint}

/**
  * CRSBounds captures lower left and upper right extreme points for a given
  * CRS. Extreme points are provided as MosaicPoints. The CRSBounds instances
  * are constructed via geometry API.
 *
 * @param lowerLeft
  *   Lower left extreme point (xmin, ymin).
  * @param upperRight
  *   Upper right extreme point (xmax, ymax).
  */
case class CRSBounds(lowerLeft: JTSPoint, upperRight: JTSPoint)

object CRSBounds {

    /**
      * Construct CRSBounds instance for give extreme coordinate values.
      * Construction is bound for the selected geometry API at runtime.
      * @param x1
      *   Minimum x coordinate value.
      * @param y1
      *   Minimum y coordinate value.
      * @param x2
      *   Maximum x coordinate value.
      * @param y2
      *   Maximum y coordinate value.
      * @return
      */
    def apply(x1: Double, y1: Double, x2: Double, y2: Double): CRSBounds = {
        CRSBounds(JTS.fromCoords(Seq(x1, y1)).asInstanceOf[JTSPoint], JTS.fromCoords(Seq(x2, y2)).asInstanceOf[JTSPoint])
    }
}
