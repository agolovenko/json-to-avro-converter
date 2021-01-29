package org.echo.avro

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import java.util
import scala.collection.JavaConverters._

class MapSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", createMap(create(Type.INT)))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val schemaWithDefault = new Parser().parse("""
      |{
      |  "type": "record",
      |  "name": "sch_rec2",
      |  "fields": [
      |    {
      |     "name": "field2", 
      |     "type": {
      |       "type": "map",
      |       "values": "int"
      |     },
      |     "default": {"one": 1, "two": 2,"three": 3}
      |    }
      |  ]
      |}""".stripMargin)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": {"twelve": 12, "fourteen": 14}}""")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)
    val expected = new util.HashMap[String, Integer]()
    expected.put("twelve", 12)
    expected.put("fourteen", 14)
    record.get("field1") should ===(expected)
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": [1]}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on wrong value type" in {
    val data = Json.parse("""{"field1": {"one": "1"}}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonConverter().parse(data, schemaWithDefault)

    ReflectData.get().validate(schemaWithDefault, record) should ===(true)
    val expected = new util.HashMap[String, Integer]()
    expected.put("one", 1)
    expected.put("two", 2)
    expected.put("three", 3)
    record.get("field2") should ===(expected)
  }
}
