package name.dzema

import java.io.ByteArrayOutputStream

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.{DecoderFactory, EncoderFactory, BinaryEncoder}

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
    val payload = out.toByteArray

    println(s"=> Encoded record with schema V1: $genericV1Record into byte array '${out.toString}'\n")

    println("=> Decoding with schema where new field is added in the middle")
    val schemaWithNewFieldInTheMiddle = SchemaBuilder
      .record("TestSchema").namespace("com.fyber.test")
      .fields()
      .name("f1").`type`.stringType().noDefault()
      .name("new_field").`type`.stringType().stringDefault("default value of new field")
      .name("f2").`type`.intType().noDefault()
      .endRecord()

    val vcDatumReader = new GenericDatumReader[GenericRecord](schemaWithNewFieldInTheMiddle)
    println("==> Using binaryDecoder")
    try {
      vcDatumReader.read(null, df.binaryDecoder(payload, null))
    } catch {
      case e: Exception => println(s"Got exception: $e\n")
    }

    println("==> Using resolving binaryDecoder")
    try {
      val vcRdDecoder = df.resolvingDecoder(schemaV1, schemaWithNewFieldInTheMiddle, df.binaryDecoder(payload, null))
      vcDatumReader.read(null, vcRdDecoder)
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




    println("=> Decoding with schema where new field is added at the end")

    println("==> Using binaryDecoder")
    try {
      datumReader.read(null, df.binaryDecoder(payload, null))
    } catch {
      case e: Exception => println(s"Got exception: $e\n")
    }

    println("==> Using resolving binaryDecoder")
    val decoder = df.resolvingDecoder(schemaV1, compatibleSchema, df.binaryDecoder(payload, null))
    val record = datumReader.read(null, decoder)
    println(s"Got record: $record")

    /*
     * Morales of the story:
     *   1. Add new fields to the end of the schema if you want it to be backward compatible.
     *   2. Use resolving decoder which confluent wire format makes possible.
     */
  }
}