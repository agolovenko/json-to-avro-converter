package io.github.agolovenko.avro

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.collection.JavaConverters._

class LongSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", create(Type.LONG))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val fieldWithDefault  = new Field("field2", create(Type.LONG), doc, 12147483647L)
  private val schemaWithDefault = createRecord("sch_rec2", doc, ns, false, Seq(fieldWithDefault).asJava)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": 92147483647}""")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===(92147483647L)
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on wrong type" in {
    val data = Json.parse("""{"field1": "12"}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "fails on wrong numeric type" in {
    val data = Json.parse("""{"field1": 12.45}""")
    a[WrongTypeException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonConverter().parse(data, schemaWithDefault)

    ReflectData.get().validate(schema, record) should ===(true)
    record.get("field2") should ===(12147483647L)
  }
}
