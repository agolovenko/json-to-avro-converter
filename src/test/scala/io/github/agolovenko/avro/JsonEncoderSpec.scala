package io.github.agolovenko.avro

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.apache.avro.util.RandomData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._

class JsonEncoderSpec extends AnyWordSpec with Matchers {
  import StringEncoders._

  import DateTimeFormatter._

  private val schema = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec",
      |  "fields": [
      |    {
      |      "name": "f_record",
      |      "type": {
      |        "name": "sch_f_record",
      |        "type": "record",
      |        "fields": [
      |          {
      |            "name": "nf_string",
      |            "type": "string"
      |          },
      |          {
      |            "name": "nf_int",
      |            "type": "int"
      |          }
      |        ]
      |      }
      |    },
      |    {
      |      "name": "f_array",
      |      "type": {
      |        "type": "array",
      |        "items": "int"
      |      }
      |    },
      |    {
      |      "name": "f_map",
      |      "type": {
      |        "type": "map",
      |        "values": "int"
      |      }
      |    },
      |    {
      |      "name": "f_enum",
      |      "type": {
      |        "type": "enum",
      |        "name": "Suit",
      |        "symbols": [
      |          "SPADES",
      |          "HEARTS",
      |          "DIAMONDS",
      |          "CLUBS"
      |        ]
      |      }
      |    },
      |    {
      |      "name": "f_union",
      |      "type": [
      |        "int",
      |        "boolean"
      |      ]
      |    },
      |    {
      |      "name": "f_long",
      |      "type": "long"
      |    },
      |    {
      |      "name": "f_float",
      |      "type": "float"
      |    },
      |    {
      |      "name": "f_double",
      |      "type": "double"
      |    },
      |    {
      |      "name": "f_boolean",
      |      "type": "boolean"
      |    },
      |    {
      |      "name": "f_bytes",
      |      "type": "bytes"
      |    },
      |    {
      |      "name": "f_fixed",
      |      "type": {
      |        "name": "md5",
      |        "type": "fixed",
      |        "size": 16
      |      }
      |    },
      |    {
      |      "name": "f_date",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "date"
      |      }
      |    }
      |  ]
      |}""".stripMargin)

  "produces json for RandomData" in {
    val encoder = new JsonEncoder(base64Encoders ++ dateEncoder(ISO_DATE))

    val rec = toRecord(toBytes(new RandomData(schema, 1).asScala.head.asInstanceOf[GenericData.Record]), schema)

    val jsons = new RandomData(schema, 1 << 10).asScala.map { r =>
      val record = r.asInstanceOf[GenericData.Record]
      record -> encoder(record)
    }

    println(jsons.size)
  }
}
