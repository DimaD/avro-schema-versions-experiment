package name.dzema
import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

/**
  * Created by dzema on 16/03/16.
  */
trait CompatibilityDemonstration {
  lazy val df = DecoderFactory.get()

  def encodeToBinaryAvro(schema: Schema, record: GenericRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.directBinaryEncoder(out, null)
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)

    datumWriter.write(record, encoder)
    out.toByteArray
  }

  def decodeBinaryAvro(schema: Schema, payload: Array[Byte]): GenericRecord = {
    val datumReader = new GenericDatumReader[GenericRecord](schema)

    datumReader.read(null, df.binaryDecoder(payload, null))
  }

  def decodeAndResolveBinaryAvro(writerSchema: Schema, resolveTo: Schema, payload: Array[Byte]): GenericRecord = {
    val datumReader = new GenericDatumReader[GenericRecord](resolveTo)
    val decoder = df.resolvingDecoder(writerSchema, resolveTo, df.binaryDecoder(payload, null))

    datumReader.read(null, decoder)
  }

  def demonstrateFailure(description: String)(f: => Any): Unit = {
    println(s"==> $description")

    try {
      f
    } catch {
      case e: Exception => println(s"  Got exception: $e\n")
    }
  }

  def demonstrateSuccess(description: String)(f: => GenericRecord): Unit = {
    println(s"==> $description")
    println(s"  Got record: ${f}")
  }
}
