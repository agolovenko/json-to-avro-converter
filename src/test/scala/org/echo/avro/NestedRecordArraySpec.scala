package org.echo.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.collection.JavaConverters._

class NestedRecordArraySpec extends AnyWordSpec with Matchers {
  import Schema._

  private val schema = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec1",
      |  "fields": [
      |    {
      |     "name": "field1",
      |     "type": {
      |       "name": "sch_n_arr2",
      |       "type": "array",
      |       "items": {
      |         "name": "sch_n_rec2",
      |         "type": "record",
      |         "fields": [
      |           {
      |             "name": "n_field1",
      |             "type": "string"
      |           },
      |           {
      |             "name": "n_field2",
      |             "type": "int"
      |           }
      |          ]
      |        }
      |      },
      |      "default": [{"n_field1": "bbb", "n_field2": 33}]
      |    }
      |  ]
      |}""".stripMargin)

  "parses nested record array correctly" in {
    val data   = Json.parse("""{"field1": [{"n_field1": "aaa", "n_field2": 23}]}""")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)

    val rec1 = new GenericData.Record(schema.getField("field1").schema().getElementType)
    rec1.put("n_field1", "aaa")
    rec1.put("n_field2", 23)
    val expected = new GenericData.Array(schema.getField("field1").schema(), Seq(rec1).asJava)
    record.get("field1") should ===(expected)
  }

  "applies default value to nested record array" in {
    val data   = Json.parse("{}")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)

    val rec1 = new GenericData.Record(schema.getField("field1").schema().getElementType)
    rec1.put("n_field1", "bbb")
    rec1.put("n_field2", 33)
    val expected = new GenericData.Array(schema.getField("field1").schema(), Seq(rec1).asJava)
    record.get("field1") should ===(expected)
  }
}