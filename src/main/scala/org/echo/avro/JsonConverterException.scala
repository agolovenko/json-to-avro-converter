package org.echo.avro

import play.api.libs.json.JsValue

import scala.collection.mutable

class JsonConverterException(message: String, path: mutable.ArrayStack[String]) extends RuntimeException(s"$message @ ${path.reverse.mkString("/", "/", "")}")

class WrongTypeException(expectedType: String, actual: JsValue, path: mutable.ArrayStack[String])
    extends JsonConverterException(s"Failed to extract $expectedType from $actual", path)

class MissingValueException(expectedType: String, path: mutable.ArrayStack[String]) extends JsonConverterException(s"Missing $expectedType node", path)
