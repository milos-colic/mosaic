package com.databricks.labs.mosaic.expressions

import com.databricks.labs.mosaic.core.jts.{JTSGeometry, JTSPoint}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

object SpatialSQLAPIsMock {

    def st_xmax: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_XMax
            val geom = JTSGeometry.fromWKT(geometry)
            geom.minMaxCoord("X", "MAX")
        })

    def st_xmin: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_XMin
            val geom = JTSGeometry.fromWKT(geometry)
            geom.minMaxCoord("X", "MIN")
        })

    def st_ymax: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_YMax
            val geom = JTSGeometry.fromWKT(geometry)
            geom.minMaxCoord("Y", "MAX")
        })

    def st_ymin: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_YMin
            val geom = JTSGeometry.fromWKT(geometry)
            geom.minMaxCoord("Y", "MIN")
        })

    def st_transform: UserDefinedFunction =
        functions.udf((geometry: String, targetSRID: Int) => {
            // Mock implementation of ST_Transform
            val geom = JTSGeometry.fromWKT(geometry)
            geom.transformCRSXY(targetSRID)
        })

    def st_asgeojson: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_AsGeoJSON
            val geom = JTSGeometry.fromWKT(geometry)
            geom.toJSON
        })

    def st_centroid: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_Centroid
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getCentroid.toWKT
        })

    def st_translate: UserDefinedFunction =
        functions.udf((geometry: String, x: Double, y: Double) => {
            // Mock implementation of ST_Translate
            val geom = JTSGeometry.fromWKT(geometry)
            geom.translate(x, y).toWKT
        })

    def st_x: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_X
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getCentroid.getX
        })

    def st_y: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_Y
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getCentroid.getY
        })

    def st_z: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_Z
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getCentroid.getZ
        })

    def st_point: UserDefinedFunction =
        functions.udf((x: Double, y: Double) => {
            // Mock implementation of ST_Point
            JTSPoint.fromWKT(s"POINT($x $y)").toWKT
        })

    def st_geomfromwkt: UserDefinedFunction =
        functions.udf((wkt: String) => {
            // Mock implementation of ST_GeomFromWKT
            JTSGeometry.fromWKT(wkt).toWKT
        })

    def st_buffer: UserDefinedFunction =
        functions.udf((geometry: String, distance: Double) => {
            // Mock implementation of ST_Buffer
            val geom = JTSGeometry.fromWKT(geometry)
            geom.buffer(distance).toWKT
        })

    def st_intersects: UserDefinedFunction =
        functions.udf((geometry1: String, geometry2: String) => {
            // Mock implementation of ST_Intersects
            val geom1 = JTSGeometry.fromWKT(geometry1)
            val geom2 = JTSGeometry.fromWKT(geometry2)
            geom1.intersects(geom2)
        })

    def st_geometrytype: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_GeometryType
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getGeometryType
        })

    def st_aswkt: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_AsWKT
            val geom = JTSGeometry.fromWKT(geometry)
            geom.toWKT
        })

    def st_aswkb: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_AsWKB
            val geom = JTSGeometry.fromWKT(geometry)
            geom.toWKB
        })

    def st_geomfromgeojson: UserDefinedFunction =
        functions.udf((geojson: String) => {
            // Mock implementation of ST_GeomFromGeoJSON
            val geom = JTSGeometry.fromJSON(geojson)
            geom.toWKT
        })

    def st_geomfromwkb: UserDefinedFunction =
        functions.udf((wkb: Array[Byte]) => {
            // Mock implementation of ST_GeomFromWKB
            val geom = JTSGeometry.fromWKB(wkb)
            geom.toWKT
        })

    def st_setsrid: UserDefinedFunction =
        functions.udf((geometry: String, srid: Int) => {
            // Mock implementation of ST_SetSRID
            val geom = JTSGeometry.fromWKT(geometry)
            geom.setSpatialReference(srid)
            geom.toJSON
        })

    def st_area: UserDefinedFunction =
        functions.udf((geometry: String) => {
            // Mock implementation of ST_Area
            val geom = JTSGeometry.fromWKT(geometry)
            geom.getArea
        })

}
