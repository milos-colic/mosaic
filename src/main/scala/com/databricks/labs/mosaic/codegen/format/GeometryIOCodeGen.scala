package com.databricks.labs.mosaic.codegen.format

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

trait GeometryIOCodeGen {

    def fromWKT(ctx: CodegenContext, eval: String): (String, String)

    def fromWKB(ctx: CodegenContext, eval: String): (String, String)

    def fromJSON(ctx: CodegenContext, eval: String): (String, String)

    def fromHex(ctx: CodegenContext, eval: String): (String, String)

    def fromInternal(ctx: CodegenContext, eval: String): (String, String)

    def toWKT(ctx: CodegenContext, eval: String): (String, String)

    def toWKB(ctx: CodegenContext, eval: String): (String, String)

    def toJSON(ctx: CodegenContext, eval: String): (String, String)

    def toGeoJSON(ctx: CodegenContext, eval: String): (String, String)

    def toHEX(ctx: CodegenContext, eval: String): (String, String)

    def toInternal(ctx: CodegenContext, eval: String): (String, String)

}
