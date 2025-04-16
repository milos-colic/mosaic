package com.databricks.labs.mosaic.sql

import java.util.Locale
import scala.util.Try
import com.databricks.labs.mosaic.functions.MosaicContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object Prettifier {

    def prettified(df: DataFrame, columnNames: Option[List[String]] = None): DataFrame = {
        val mosaicContext = MosaicContext.context()
        import mosaicContext.functions._

        val keywords = List("WKB_", "_WKB", "_HEX", "HEX_", "COORDS_", "_COORDS", "POLYGON", "POINT", "GEOMETRY")
        val explicitColumns = columnNames.getOrElse(List())

        val casted = df.columns
            .map(colName =>
                Try {
                    if (explicitColumns.contains(colName)) {
                        expr(s"st_aswkt($colName)")
                    } else if (
                      keywords.exists(kw => colName.toUpperCase(Locale.ROOT).contains(kw)) &
                          !colName.toUpperCase(Locale.ROOT).contains("INDEX")
                    ) {
                        expr(s"st_aswkt($colName)").alias(s"WKT($colName)")
                    } else {
                        col(colName)
                    }
                }.getOrElse(col(colName))
            )
            .toSeq

        df.select(casted: _*)
    }

}
