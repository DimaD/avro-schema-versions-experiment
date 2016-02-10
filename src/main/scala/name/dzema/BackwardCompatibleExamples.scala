package name.dzema

import java.io.ByteArrayOutputStream

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory, BinaryEncoder}

/**
  * Created by dzema on 08/02/16.
  */
object BackwardCompatibleExamples {
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

  val v1DatumWriter =  new GenericDatumWriter[GenericRecord](schemaV1)

  val df = DecoderFactory.get()

  def main(args: Array[String]): Unit = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.directBinaryEncoder(out, null)
    v1DatumWriter.write(genericV1Record, encoder)

    println(s"=> Encoded record with schema V1: $genericV1Record into byte array '${out.toString}'\n")

    println("=> Decoding with schema where new field is added in the middle")
    try {
      val visuallyCompatibleSchema = SchemaBuilder
        .record("TestSchema").namespace("com.fyber.test")
        .fields()
        .name("f1").`type`.stringType().noDefault()
        .name("new_field").`type`.stringType().stringDefault("default value of new field")
        .name("f2").`type`.intType().noDefault()
        .endRecord()

      val datumReader = new GenericDatumReader[GenericRecord](visuallyCompatibleSchema)
      datumReader.read(null, df.binaryDecoder(out.toByteArray, null))
    } catch {
      case e: Exception => println(s"Got exception: $e\n")
    }

    val compatibleSchema = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("f2").`type`.intType().noDefault()
      .name("new_field").`type`.stringType().stringDefault("default value of new field")
      .endRecord()
    val datumReader = new GenericDatumReader[GenericRecord](compatibleSchema)

    println("=> Decoding directly with schema where new field is added at the end")

    try {
      datumReader.read(null, df.binaryDecoder(out.toByteArray, null))
    } catch {
      case e: Exception => println(s"Got exception: $e\n")
    }

    println("=> Decoding with resolving decoder:")
    val decoder = df.resolvingDecoder(schemaV1, compatibleSchema, df.binaryDecoder(out.toByteArray, null))
    val record = datumReader.read(null, decoder)
    println(s"Got record: $record")

    /*
     * Morales of the story:
     *   1. Add new fields to the end of the schema if you want it to be backward compatible.
     *   2. Use resolving decoder which confluent wire format makes possible.
     */
  }
}