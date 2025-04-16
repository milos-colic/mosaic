package com.databricks.labs.mosaic.codegen.format

import com.databricks.labs.mosaic.core.jts.{GeometryFormat, JTS}
import com.databricks.labs.mosaic.core.types._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

object ConvertToCodeGen {

    // noinspection DuplicatedCode
    def doCodeGen(
        ctx: CodegenContext,
        ev: ExprCode,
        nullSafeCodeGen: (CodegenContext, ExprCode, String => String) => ExprCode,
        inputDataType: DataType,
        outputDataTypeName: String
    ): ExprCode = {
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
              if (inputDataType.simpleString == outputDataTypeName) {
                  s"""
                     |${ev.value} = $eval;
                     |""".stripMargin
              } else {
                  val (inCode, geomInRef) = readGeometryCode(ctx, eval, inputDataType)
                  val (outCode, geomOutRef) = writeGeometryCode(ctx, geomInRef, outputDataTypeName)
                  JTS.codeGenTryWrap(s"""
                                                |$inCode
                                                |$outCode
                                                |${ev.value} = $geomOutRef;
                                                |""".stripMargin)
              }
          }
        )
    }

    // noinspection DuplicatedCode
    def readGeometryCode(ctx: CodegenContext, eval: String, inputDataType: DataType): (String, String) = {
        val geometryCodeGen = JTS.ioCodeGen
        inputDataType match {
            case BinaryType           => geometryCodeGen.fromWKB(ctx, eval)
            case StringType           => geometryCodeGen.fromWKT(ctx, eval)
            case HexType              => geometryCodeGen.fromHex(ctx, eval)
            case JSONType             => geometryCodeGen.fromJSON(ctx, eval)
            case InternalGeometryType => geometryCodeGen.fromInternal(ctx, eval)
            case _                    => throw new Error(s"Geometry API unsupported: ${inputDataType.typeName}.")
        }
    }

    // noinspection DuplicatedCode
    def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataType: DataType): (String, String) = {
        val outDataFormat = GeometryFormat.getDefaultFormat(outputDataType)
        writeGeometryCode(ctx, eval, outDataFormat)
    }

    // noinspection DuplicatedCode
    def writeGeometryCode(ctx: CodegenContext, eval: String, outputDataFormatName: String): (String, String) = {
        val geometryCodeGen = JTS.ioCodeGen

        outputDataFormatName match {
            case "WKB"        => geometryCodeGen.toWKB(ctx, eval)
            case "WKT"        => geometryCodeGen.toWKT(ctx, eval)
            case "HEX"        => geometryCodeGen.toHEX(ctx, eval)
            case "JSONOBJECT" => geometryCodeGen.toJSON(ctx, eval)
            case "GEOJSON"    => geometryCodeGen.toGeoJSON(ctx, eval)
            case "COORDS"     => geometryCodeGen.toInternal(ctx, eval)
            case _            => throw new Error(s"Data type unsupported: $outputDataFormatName.")
        }
    }

}
