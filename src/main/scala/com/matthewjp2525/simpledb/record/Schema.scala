package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.record.FieldType.*

import scala.compiletime.ops.string.Length

type FieldName = String

enum FieldType(val value: Int):
  case Integer extends FieldType(0)
  case Varchar extends FieldType(1)

object FieldType:
  def fromValue(value: Int): FieldType =
    value match
      case 0 => Integer
      case 1 => Varchar
      case otherValue =>
        throw new RuntimeException(s"field type for value $otherValue not supported")

case class FieldInfo(`type`: FieldType, length: Int)

case class Schema(
                   fields: Vector[FieldName] = Vector.empty[FieldName],
                   info: Map[FieldName, FieldInfo] = Map.empty[FieldName, FieldInfo]
                 )

object SchemaOps:
  extension (schema: Schema)
    def `type`(fieldName: FieldName): FieldType =
      schema.info(fieldName).`type`

    def length(fieldName: FieldName): Int =
      schema.info(fieldName).length

    def hasField(fieldName: FieldName): Boolean =
      schema.fields.contains(fieldName)

    def addField(fieldName: FieldName, `type`: FieldType, length: Int): Schema =
      val newFields = (schema.fields :+ fieldName).distinct
      val newInfo = schema.info + (fieldName -> FieldInfo(`type`, length))
      schema.copy(fields = newFields, info = newInfo)

    def addIntField(fieldName: FieldName): Schema =
      addField(fieldName, Integer, 0)

    def addStringField(fieldName: FieldName, length: Int): Schema =
      addField(fieldName, Varchar, length)

    def add(fieldName: FieldName, aSchema: Schema): Schema =
      val `type` = aSchema.`type`(fieldName)
      val length = aSchema.length(fieldName)
      addField(fieldName, `type`, length)

    def addAll(aSchema: Schema): Schema =
      val newFields = (schema.fields ++ aSchema.fields).distinct
      val newInfo = schema.info ++ aSchema.info
      schema.copy(fields = newFields, info = newInfo)
