package name.dzema

import java.io.ByteArrayOutputStream

import name.dzema.BackwardCompatibleExamples._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory, BinaryEncoder}

object ChangeTypeIntToLong extends CompatibilityDemonstration {
  val schemaV1 = SchemaBuilder
    .record("TestSchema").namespace("com.fyber.test")
    .fields()
    .name("f1").`type`.stringType().noDefault()
    .name("f2").`type`.intType().noDefault()
    .name("f3").`type`.intType().noDefault()
    .endRecord()

  val genericV1Record = new GenericRecordBuilder(schemaV1).
    set("f1", "field 1 value").
    set("f2", 42).
    set("f3", 84).
    build()

  val payload = encodeToBinaryAvro(schemaV1, genericV1Record)

  def main(args: Array[String]): Unit = {
    println("=> Backward compatibility of type change Int -> Long")

    val typeChangedSchema = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("f2").`type`.longType().noDefault()
      .name("f3").`type`.intType().noDefault()
      .endRecord()

    println("=> Decoding with schema where data type is changed")

    demonstrateSuccess("Using binaryDecoder") {
      decodeBinaryAvro(typeChangedSchema, payload)
    }

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, typeChangedSchema, payload)
    }

    println("\n----------------------------------------\n=> Forward compatibility of type change Int -> Long")
    val genericV2Record = new GenericRecordBuilder(typeChangedSchema).
      set("f1", "field 1 value").
      set("f2", 2147483647L + 1). // 2147483647L is the biggest Integer
      //set("f2", java.lang.Long.MAX_VALUE).
      //set("f2", 84).
      set("f3", 42).
      build()

    val payloadV2 = encodeToBinaryAvro(typeChangedSchema, genericV2Record)

    println("=> Decoded with it's own schema")
    demonstrateSuccess("Using binaryDecoder") {
      decodeBinaryAvro(typeChangedSchema, payloadV2)
    }

    println("=> Decoded using old schema")
    demonstrateSuccess("Using binaryDecoder") {
      decodeBinaryAvro(schemaV1, payloadV2)
    }

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, typeChangedSchema, payloadV2)
    }
  }
}