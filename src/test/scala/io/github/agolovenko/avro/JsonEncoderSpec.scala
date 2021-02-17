package io.github.agolovenko.avro

import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}

class JsonEncoderSpec extends AnyWordSpec with Matchers {
  import RandomData._
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
      |    },
      |    {
      |      "name": "f_time",
      |      "type": {
      |        "type": "int",
      |        "logicalType": "time-millis"
      |      }
      |    },
      |    {
      |      "name": "f_timestamp",
      |      "type": {
      |        "type": "long",
      |        "logicalType": "timestamp-millis"
      |      }
      |    }
      |  ]
      |}""".stripMargin)

  "produces json for RandomData" in {
    val encoder = new JsonEncoder(
      base64Encoders
        ++ dateEncoder(ISO_DATE)
        ++ timeEncoders(ISO_LOCAL_TIME)
        ++ dateTimeEncoders(ISO_LOCAL_DATE_TIME, ZoneId.of("CET"))
    )

    val fromDate   = LocalDate.of(2020, 1, 1)
    val generators = dateGenerator(fromDate, 1 << 10) ++ timeGenerators ++ dateTimeGenerators(fromDate, 1 << 10, ZoneId.of("UTC"))
    val conf       = RandomDataConf(schema, total = 1 << 10, generators)
    val jsons = new RandomData(conf).map { r =>
      val record = r.asInstanceOf[GenericData.Record]
      record -> encoder(record)
    }.toVector
  }
}
