package name.dzema

import java.io.ByteArrayOutputStream

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory, BinaryEncoder}

object BackwardCompatibleExamples extends CompatibilityDemonstration {
  val schemaV1 = SchemaBuilder
    .record("TestSchema").namespace("com.fyber.test")
    .fields()
    .name("f1").`type`.stringType().noDefault()
    .name("f2").`type`.intType().noDefault()
    .endRecord()

  val genericV1Record = new GenericRecordBuilder(schemaV1).
    set("f1", "field 1 value").
    set("f2", 42).
    build()

  def main(args: Array[String]): Unit = {
    val payload = encodeToBinaryAvro(schemaV1, genericV1Record)

    println(s"=> Encoded record with schema V1: $genericV1Record into byte array '${new String(payload)}'\n")

    println("=> Decoding with schema where new field is added in the middle")
    val schemaWithNewFieldInTheMiddle = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("new_field").`type`.stringType().stringDefault("default value of new field")
      .name("f2").`type`.intType().noDefault()
      .endRecord()

    demonstrateFailure("Using binaryDecoder") {
      decodeBinaryAvro(schemaWithNewFieldInTheMiddle, payload)
    }

    demonstrateFailure("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, schemaWithNewFieldInTheMiddle, payload)
    }

    val compatibleSchema = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("f2").`type`.intType().noDefault()
      .name("new_field").`type`.stringType().stringDefault("default value of new field")
      .endRecord()

    println("=> Decoding with schema where new field is added at the end")

    demonstrateFailure("Using binaryDecoder") {
      decodeBinaryAvro(compatibleSchema, payload)
    }

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, compatibleSchema, payload)
    }

    println("\n=> Decoding with schema where field was dropped")
    val compatibleSchemaWithoutAField = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .endRecord()

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, compatibleSchemaWithoutAField, payload)
    }

    /*
     * Morales of the story:
     *   1. Add new fields to the end of the schema if you want it to be backward compatible.
     *   2. Use resolving decoder which confluent wire format makes possible.
     */


   println("\n----------------------------------------\n=> Compatibility of enum values")

    val enumSchemaV1 = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("field", "b").noDefault()
      .endRecord()

    val enumSchema = enumSchemaV1.getField("f1").schema()
    val enumRecord = new GenericRecordBuilder(enumSchemaV1).
      set("f1", new GenericData.EnumSymbol(enumSchema, "field")).
      build()

    val enumPayload = encodeToBinaryAvro(enumSchemaV1, enumRecord)

    println(s"=> Encoded record with enumSchemaV1: $enumRecord into byte array '${new String(enumPayload)}'\n")
    println("==> Decoding to a schema where encoded enum symbol is renamed")
    val enumSchemaV2 = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("field2", "b").noDefault()
      .endRecord()

    demonstrateSuccess("using binary decoder") {
      decodeBinaryAvro(enumSchemaV2, enumPayload)
    }

    demonstrateFailure("using resolving decoder") {
      decodeAndResolveBinaryAvro(enumSchemaV1, enumSchemaV2, enumPayload)
    }

    val enumSchemaV3 = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("newField", "field", "b").noDefault()
      .endRecord()


    println("==> Decoding to a schema where new enum symbol is added to a start of enumeration")
    demonstrateSuccess("using binary decoder") {
      decodeBinaryAvro(enumSchemaV3, enumPayload)
    }

    demonstrateSuccess("using resolving decoder") {
      decodeAndResolveBinaryAvro(enumSchemaV1, enumSchemaV3, enumPayload)
    }

    /*
     * Morales of this other story:
     *   1. Renaming enum symbols is not a compatible change when resolving schemas
     *   2. Binary decoder is fooled when enum symbol is renamed or new one is added not in the end position of enumeration
     */

    println("\n----------------------------------------\n=> Compatibility of data type changes")

    val typeChangedSchema = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("f2").`type`.longType().noDefault()
      .endRecord()

    println("=> Decoding with schema where data type is changed")

    demonstrateSuccess("Using binaryDecoder") {
      decodeBinaryAvro(typeChangedSchema, payload)
    }

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, typeChangedSchema, payload)
    }

    println("=> For data encoded with new schema")
    val schemaV2 = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("f2").`type`.longType().noDefault()
      .endRecord()

    val genericV2Record = new GenericRecordBuilder(schemaV2).
      set("f1", "field 1 value").
      set("f2", 42).
      build()

    val payloadV2 = encodeToBinaryAvro(schemaV2, genericV2Record)

    println("=> When decoding trying to decode it with old schema")
    demonstrateSuccess("Using binaryDecoder") {
      decodeBinaryAvro(schemaV1, payload)
    }

    demonstrateSuccess("Using resolving binaryDecoder") {
      decodeAndResolveBinaryAvro(schemaV1, typeChangedSchema, payload)
    }
  }
}
