package io.github.agolovenko.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.collection.JavaConverters._

class FixedSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", createFixed("sch_fix1", doc, ns, 3))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val schemaWithDefault = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec2",
      |  "fields": [
      |    {
      |     "name": "field2", 
      |     "type": {
      |       "name": "sch_fix1",
      |       "type": "fixed",
      |       "size": 3
      |     },
      |     "default": "\u00FF\uFF00\u00FF"
      |    }
      |  ]
      |}""".stripMargin)

  "parses correctly" in {
    val data   = Json.parse(s"""{"field1": "${toBase64(Array[Byte](1, 2, 3))}"}""")
    val record = new JsonParser(StringParsers.base64Parsers)(data, schema)

    GenericData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(new GenericData.Fixed(field.schema(), Array[Byte](1, 2, 3)))
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonParser(StringParsers.base64Parsers)(data, schema)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": 1}""")
    a[WrongTypeException] should be thrownBy new JsonParser(StringParsers.base64Parsers)(data, schema)
  }

  "fails on non-base64 string" in {
    val data = Json.parse("""{"field1": "1"}""")
    a[WrongTypeException] should be thrownBy new JsonParser(StringParsers.base64Parsers)(data, schema)
  }

  "fails on wrong size" in {
    val data = Json.parse(s"""{"field1": "${toBase64(Array[Byte](1, 2))}"}""")
    a[WrongTypeException] should be thrownBy new JsonParser(StringParsers.base64Parsers)(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonParser(StringParsers.base64Parsers)(data, schemaWithDefault)

    GenericData.get().validate(schemaWithDefault, record) should ===(true)
    record.get("field2") should ===(new GenericData.Fixed(field.schema(), Array[Byte](-1, 63, -1)))
  }
}
