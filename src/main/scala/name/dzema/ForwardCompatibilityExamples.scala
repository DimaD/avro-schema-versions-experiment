package name.dzema

import name.dzema.BackwardCompatibleExamples._
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}

/**
  * Created by dzema on 16/03/16.
  */
object ForwardCompatibilityExamples extends CompatibilityDemonstration {
  def main(args: Array[String]) {
    println("\n----------------------------------------\n=> Forward compatibility of enum values")

    val forwardEnumSchemaA = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("a", "b").noDefault()
      .endRecord()

    val forwardEnumSchemaAwithDefault = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("a", "b").enumDefault("a")
      .endRecord()

    println("==> Decoding from a schema where enum symbol is added")
    val forwardEnumSchemaB = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.enumeration("f1").symbols("a", "b", "c").noDefault()
      .endRecord()

    val forwardEnumSchema = forwardEnumSchemaB.getField("f1").schema()
    val forwardEnumRecord = new GenericRecordBuilder(forwardEnumSchemaB).
      set("f1", new GenericData.EnumSymbol(forwardEnumSchemaB, "c")).
      build()

    val forwardEnumPayload = encodeToBinaryAvro(forwardEnumSchemaB, forwardEnumRecord)
    println(s"=> Encoded record with forwardEnumSchemaB: $forwardEnumRecord into byte array '${new String(forwardEnumPayload)}'\n")

    demonstrateFailure("using resolving decoder into schema without default value") { () =>
      decodeAndResolveBinaryAvro(forwardEnumSchemaB, forwardEnumSchemaA, forwardEnumPayload)
    }

    demonstrateFailure("using resolving decoder into schema with default value") { () =>
      decodeAndResolveBinaryAvro(forwardEnumSchemaB, forwardEnumSchemaAwithDefault, forwardEnumPayload)
    }
  }
}
