package io.github.agolovenko.avro

import io.github.agolovenko.avro.JsonParser.typeName
import org.apache.avro.Schema
import play.api.libs.json.JsValue

import scala.collection.mutable

class JsonParserException(message: String)(implicit path: mutable.ArrayStack[String])
    extends RuntimeException(s"$message @ ${path.reverse.mkString("/", "/", "")}")

class WrongTypeException(schema: Schema, value: JsValue, expl: Option[String] = None)(implicit path: mutable.ArrayStack[String])
    extends JsonParserException(s"Failed to extract ${typeName(schema)} from $value${expl.fold("")(": " + _)}")

class MissingValueException(schema: Schema)(implicit path: mutable.ArrayStack[String]) extends JsonParserException(s"Missing ${typeName(schema)} node")
