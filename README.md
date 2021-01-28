# json-to-avro-converter

Converter of JSON format to AVRO `GenericData.Record` written in Scala using Play-Json

### Features

* unions without avro wrapping
* schema's default values are used if data is missing
* data not present in schema is ignored
* currently not supporting `BYTES` and `FIXED` types

### Usage

```scala
import agolovenko.avro._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import play.api.libs.json.{JsValue, Json}

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

val data: JsValue              = Json.parse("""{"field1": [12, 14]}""")
val record: GenericData.Record = new AvroParser().parse(data, schema)
val bytes: Array[Byte]         = toBytes(record)
```

For more examples check out the tests!
