package org.echo.avro

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

import scala.collection.JavaConverters._
class StringSpec extends AnyWordSpec with Matchers {
  import Schema._

  private val doc               = "no-doc"
  private val ns                = "parser.test"
  private val field             = new Field("field1", create(Type.STRING))
  private val schema            = createRecord("sch_rec1", doc, ns, false, Seq(field).asJava)
  private val fieldWithDefault  = new Field("field2", create(Type.STRING), doc, "default")
  private val schemaWithDefault = createRecord("sch_rec2", doc, ns, false, Seq(fieldWithDefault).asJava)

  "parses correctly" in {
    val data   = Json.parse("""{"field1": "ev1"}""")
    val record = new JsonConverter().parse(data, schema)

    ReflectData.get().validate(schema, record) should ===(true)
    record.get("field1") should ===("ev1")
  }

  "fails on missing value" in {
    val data = Json.parse("{}")
    a[MissingValueException] should be thrownBy new JsonConverter().parse(data, schema)
  }

  "applies default value" in {
    val data   = Json.parse("{}")
    val record = new JsonConverter().parse(data, schemaWithDefault)

    ReflectData.get().validate(schema, record) should ===(true)
    record.get("field2") should ===("default")
  }
}
