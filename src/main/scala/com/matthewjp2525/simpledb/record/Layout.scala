package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.Page
import com.matthewjp2525.simpledb.record.FieldType.*
import com.matthewjp2525.simpledb.record.SchemaOps.*

type Offset = Int
type SlotSize = Int

case class Layout(schema: Schema, offsets: Map[FieldName, Offset], slotSize: SlotSize)

object Layout:
  def apply(schema: Schema): Layout =
    val (offsets, slotSize) =
      schema.fields.foldLeft((Map.empty[FieldName, Offset], java.lang.Integer.BYTES))((acc, fieldName) =>
        val (accOffsets, accPosition) = acc
        (accOffsets + (fieldName -> accPosition), accPosition + lengthInBytes(schema, fieldName))
      )
    Layout(schema, offsets, slotSize)

  private def lengthInBytes(schema: Schema, fieldName: FieldName): Int =
    val fieldType = schema.`type`(fieldName)
    fieldType match
      case Integer => java.lang.Integer.BYTES
      case Varchar => Page.maxLength(schema.length(fieldName))


object LayoutOps:
  extension (layout: Layout)
    def offset(fieldName: FieldName): Offset = layout.offsets(fieldName)
