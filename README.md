# json-to-avro-converter

Converter of JSON object format to AVRO `GenericData.Record` written in Scala using Play-Json

### Features

* unions as you would expect them to be in plain JSON: without avro wrapping
* schema's default values are used if data is missing
* data not present in schema is ignored
* `BYTES` and `FIXED` and some `Logical Types` are supported through [StringParsers](src/main/scala/io/github/agolovenko/avro/StringParsers.scala)
* Comprehensive Exceptions: General/Missing Value/Wrong Type. All containing JSON path and description
* Built against scala 2.11 and 2.12

### Usage
#### build.sbt:
```sbt
libraryDependencies ++= "io.github.agolovenko" %% "json-to-avro-converter" % "1.0.1"
```
#### code:

```scala
import io.github.agolovenko.avro._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json.{JsValue, Json}
import StringParsers._

val schema: Schema = new Schema.Parser().parse(
  """
    |{
    |  "type": "record",
    |  "name": "sch_rec2",
    |  "fields": [
    |    {
    |     "name": "field2", 
    |     "type": {
    |       "type": "array",
    |       "items": "int"
    |     },
    |     "default": [1, 2, 3]
    |    }
    |  ]
    |}""".stripMargin)

val data                       = Json.parse("""{"field1": [12, 14]}""")
val parser                     = new JsonParser(primitiveParsers ++ base64Parsers)
val record: GenericData.Record = parser(data, schema)
val bytes: Array[Byte]         = toBytes(record)
```

For more examples check out the [tests](src/test/scala/io/github/agolovenko/avro)!
