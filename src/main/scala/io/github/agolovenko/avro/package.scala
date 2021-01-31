package io.github.agolovenko

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}

import java.io.ByteArrayOutputStream
import java.util.Base64

package object avro {
  def toBytes(records: Seq[GenericData.Record]): Seq[Array[Byte]] = {
    val writer                 = new GenericDatumWriter[GenericData.Record]()
    var encoder: BinaryEncoder = null

    for (record <- records) yield {
      writer.setSchema(record.getSchema)

      val out = new ByteArrayOutputStream()
      encoder = EncoderFactory.get().binaryEncoder(out, encoder)
      writer.write(record, encoder)
      encoder.flush()

      out.toByteArray
    }
  }

  def toBytes(record: GenericData.Record): Array[Byte] = {
    val writer = new GenericDatumWriter[GenericData.Record]()
    writer.setSchema(record.getSchema)

    val out     = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()

    out.toByteArray
  }

  def toRecord(bytes: Array[Byte], schema: Schema): GenericData.Record = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    val reader  = new GenericDatumReader[GenericData.Record](schema)

    reader.read(null, decoder)
  }

  private[avro] def toBase64(bytes: Array[Byte]): String = new String(Base64.getEncoder.encode(bytes), "UTF-8")
}
