package org.echo.avro

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.util.Base64
import scala.collection.JavaConverters._

class BytesSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", create(Type.BYTES))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val schemaWithDefault = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec2",
      |  "fields": [
      |    {
      |     "name": "field2", 
      |     "type": "bytes",
      |     "default": "\u00FF\uFF00"
      |    }
      |  ]
      |}""".stripMargin)

  "parses correctly" in {
    val data   = Json.parse(s"""{"field1": "${toBase64(Array[Byte](1, 2, 3))}"}""")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(Array[Byte](1, 2, 3))
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": 1}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on non-base64 string" in {
    val data = Json.parse("""{"field1": "1"}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonConverter().parse(data, schemaWithDefault)

    ReflectData.get().validate(schemaWithDefault, record) should ===(true)
    record.get("field2") should ===(Array[Byte](-1, 63))
  }
}
