package agolovenko.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.{JsonProperties, Schema}
import play.api.libs.json._

import java.lang.{Boolean => JBool, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class AvroParserException(message: String, path: mutable.ArrayStack[String])
    extends IllegalArgumentException(s"$message @ ${path.reverse.mkString("/", "/", "")}")

class WrongTypeException(expectedType: String, actual: JsValue, path: mutable.ArrayStack[String])
    extends AvroParserException(s"Failed to extract $expectedType from $actual", path)

class MissingValueException(expectedType: String, path: mutable.ArrayStack[String]) extends AvroParserException(s"Missing $expectedType node", path)

class AvroParser {
  import Schema.Type._

  def parse(data: JsValue, schema: Schema): GenericData.Record = {
    val path = mutable.ArrayStack[String]()
    if (schema.getType == RECORD)
      readRecord(JsDefined(data), schema, path, defaultValue = None)
    else
      throw new AvroParserException(s"Unsupported root schema of type ${schema.getType}", path)
  }

  private def readAny(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): Any = schema.getType match {
    case RECORD => readRecord(data, schema, path, defaultValue)
    case ENUM   => readEnum(data, schema, path, defaultValue)
    case ARRAY  => readArray(data, schema, path, defaultValue)
    case MAP    => readMap(data, schema, path, defaultValue)
    case UNION  => readUnion(data, schema, path, defaultValue)

    case STRING  => read[String](data, schema, path, defaultValue)
    case INT     => read[Int](data, schema, path, defaultValue)
    case LONG    => read[Long](data, schema, path, defaultValue)
    case FLOAT   => read[Float](data, schema, path, defaultValue)
    case DOUBLE  => read[Double](data, schema, path, defaultValue)
    case BOOLEAN => read[Boolean](data, schema, path, defaultValue)

    case NULL => readNull(data, schema, path, defaultValue)

    case BYTES | FIXED => throw new AvroParserException(s"Unsupported type: ${schema.getType}", path)
  }

  private def readRecord(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): GenericData.Record = data match {
    case JsDefined(obj: JsObject) =>
      val result = new GenericData.Record(schema)
      schema.getFields.asScala.foreach { field =>
        path.push(field.name())
        val value = readAny(obj \ field.name(), field.schema(), path, Option(field.defaultVal()))
        path.pop()
        result.put(field.name(), value)
      }
      result
    case JsDefined(otherNode) => throw new WrongTypeException(RECORD.name(), otherNode, path)
    case _                    => fallbackToDefault(defaultValue, schema, path).asInstanceOf[GenericData.Record]
  }

  private def readEnum(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): GenericData.EnumSymbol = {
    val symbol = read[String](data, schema, path, defaultValue)

    if (schema.getEnumSymbols.contains(symbol)) new GenericData.EnumSymbol(schema, symbol)
    else throw new AvroParserException(s"Invalid value $symbol outside of enum ${schema.getEnumSymbols.asScala.mkString("[", ",", "]")}", path)
  }

  private def readArray(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): GenericData.Array[Any] =
    data match {
      case JsDefined(arr: JsArray) =>
        val result = new GenericData.Array[Any](arr.value.size, schema)
        arr.value.zipWithIndex.foreach {
          case (jsValue, idx) =>
            path.push(s"[$idx]")
            val value = readAny(JsDefined(jsValue), schema.getElementType, path, None)
            path.pop()
            result.add(idx, value)
        }
        result
      case JsDefined(otherNode) => throw new WrongTypeException(ARRAY.name(), otherNode, path)
      case _                    => fallbackToDefault(defaultValue, schema, path).asInstanceOf[GenericData.Array[Any]]
    }

  private def readMap(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): JMap[String, Any] = data match {
    case JsDefined(obj: JsObject) =>
      val result = new JHashMap[String, Any]()
      obj.value.foreach {
        case (key, jsValue) =>
          path.push(key)
          val value = readAny(JsDefined(jsValue), schema.getValueType, path, None)
          path.pop()
          result.put(key, value)
      }
      result
    case JsDefined(otherNode) => throw new WrongTypeException(MAP.name(), otherNode, path)
    case _                    => fallbackToDefault(defaultValue, schema, path).asInstanceOf[JMap[String, Any]]
  }

  private def readUnion(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): Any = {
    val it = schema.getTypes.asScala.iterator.zipWithIndex
      .flatMap {
        case (subSchema, idx) =>
          Try(readAny(data, subSchema, path, defaultValue.filter(_ => idx == 0))).toOption
      }

    if (it.hasNext) it.next()
    else {
      val expectedTypes = schema.getTypes.asScala.map(_.getType.name()).mkString("|")
      data match {
        case JsDefined(otherNode) => throw new WrongTypeException(expectedTypes, otherNode, path)
        case _                    => throw new MissingValueException(expectedTypes, path)
      }
    }
  }

  private def read[T: Reads](data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): T = data match {
    case JsDefined(value) =>
      value
        .validate[T]
        .fold(
          invalid = _ => throw new WrongTypeException(schema.getType.name(), value, path),
          valid = identity
        )
    case _ => fallbackToDefault(defaultValue, schema, path).asInstanceOf[T]
  }

  private def readNull(data: JsLookupResult, schema: Schema, path: mutable.ArrayStack[String], defaultValue: Option[Any]): Null = data match {
    case JsDefined(JsNull)    => null
    case JsDefined(otherNode) => throw new WrongTypeException(NULL.name(), otherNode, path)
    case _                    => fallbackToDefault(defaultValue, schema, path).asInstanceOf[Null]
  }

  private def fallbackToDefault(defaultValue: Option[Any], schema: Schema, path: mutable.ArrayStack[String]): Any =
    defaultValue.fold(throw new MissingValueException(schema.getType.name(), path)) { extractDefaultValue(_, schema, path) }

  private def extractDefaultValue(defaultValue: Any, schema: Schema, path: mutable.ArrayStack[String]): Any = (schema.getType, defaultValue) match {
    case (NULL, JsonProperties.NULL_VALUE) => null
    case (STRING, value: String)           => value
    case (ENUM, value: String)             => value
    case (INT, value: JInt)                => value.intValue()
    case (LONG, value: JLong)              => value.longValue()
    case (FLOAT, value: JFloat)            => value.floatValue()
    case (DOUBLE, value: JDouble)          => value.doubleValue()
    case (BOOLEAN, value: JBool)           => value.booleanValue()

    case (ARRAY, list: JList[_]) =>
      val extracted = list.asScala.map { extractDefaultValue(_, schema.getElementType, path) }
      new GenericData.Array(schema, extracted.asJava)
    case (MAP, map: JMap[_, _]) =>
      map.asScala.mapValues { extractDefaultValue(_, schema.getValueType, path) }.asJava
    case (RECORD, map: JMap[_, _]) =>
      val result = new GenericData.Record(schema)
      map.asScala.foreach {
        case (k, value) =>
          val key       = k.asInstanceOf[String]
          val extracted = extractDefaultValue(value, schema.getField(key).schema(), path)
          result.put(key.asInstanceOf[String], extracted)
      }
      result
    case _ => throw new AvroParserException(s"Unsupported default value $defaultValue for type ${schema.getType}", path)
  }
}
