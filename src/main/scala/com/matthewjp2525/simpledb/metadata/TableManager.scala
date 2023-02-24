package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.metadata.TableManager.MAX_NAME
import com.matthewjp2525.simpledb.metadata.TableManagerException.TableNotFoundException
import com.matthewjp2525.simpledb.record.*
import com.matthewjp2525.simpledb.record.LayoutOps.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.transaction.Transaction
import com.matthewjp2525.simpledb.util.SeqSupport.foreachTry

import scala.annotation.tailrec
import scala.util.Using

sealed abstract class TableManagerException extends Exception with Product with Serializable

object TableManagerException:
  case class TableNotFoundException(tableName: TableName) extends TableManagerException:
    override def getMessage: String = s"specified table ($tableName) not found | " + super.getMessage

class TableManager private(
                            val tableCatalogLayout: Layout,
                            val fieldCatalogLayout: Layout
                          ):

  def getLayout(tableName: TableName, tx: Transaction): Layout =
    @tailrec
    def lookupTableCatalog(ts: TableScan): SlotSize =
      if ts.next() then
        val aTableName = ts.getString("tblname")
        if aTableName == tableName then
          ts.getInt("slotsize")
        else
          lookupTableCatalog(ts)
      else
        throw TableNotFoundException(tableName)

    @tailrec
    def lookupFieldCatalog(
                            ts: TableScan,
                            accSchema: Schema,
                            accOffsets: Map[FieldName, Offset]
                          ): (Schema, Map[FieldName, Offset]) =
      if ts.next() then
        val aTableName = ts.getString("tblname")
        if aTableName == tableName then
          val fieldName = ts.getString("fldname")
          val fieldTypeValue = ts.getInt("type")
          val fieldType = FieldType.fromValue(fieldTypeValue)
          val fieldLength = ts.getInt("length")
          val offset = ts.getInt("offset")
          lookupFieldCatalog(
            ts,
            accSchema.addField(fieldName, fieldType, fieldLength),
            accOffsets + (fieldName -> offset)
          )
        else
          lookupFieldCatalog(
            ts,
            accSchema,
            accOffsets
          )
      else
        (accSchema, accOffsets)

    val slotSize = Using.resource(TableScan(tx, "tblcat", tableCatalogLayout)) { ts =>
      lookupTableCatalog(ts)
    }

    val (schema, offsets) = Using.resource(TableScan(tx, "fldcat", fieldCatalogLayout)) { ts =>
      lookupFieldCatalog(ts, Schema(), Map.empty[FieldName, Offset])
    }

    Layout(schema, offsets, slotSize)

object TableManager:
  val MAX_NAME = 16

  def apply(isNew: Boolean, tx: Transaction): TableManager =
    val tableCatalogSchema =
      Schema()
        .addStringField("tblname", MAX_NAME)
        .addIntField("slotsize")

    val tableCatalogLayout = Layout(tableCatalogSchema)

    val fieldCatalogSchema =
      Schema()
        .addStringField("tblname", MAX_NAME)
        .addStringField("fldname", MAX_NAME)
        .addIntField("type")
        .addIntField("length")
        .addIntField("offset")

    val fieldCatalogLayout = Layout(fieldCatalogSchema)

    if isNew then
      createTable(
        "tblcat",
        tableCatalogSchema,
        tableCatalogLayout,
        fieldCatalogLayout,
        tx
      )
      createTable(
        "fldcat",
        fieldCatalogSchema,
        tableCatalogLayout,
        fieldCatalogLayout,
        tx
      )

    new TableManager(tableCatalogLayout, fieldCatalogLayout)

  def createTable(
                   tableName: TableName,
                   schema: Schema,
                   tableCatalogLayout: Layout,
                   fieldCatalogLayout: Layout,
                   tx: Transaction
                 ): Unit =
    val layout = Layout(schema)

    Using.resource(TableScan(tx, "tblcat", tableCatalogLayout)) { ts =>
      ts.insert()
      ts.setString("tblname", tableName)
      ts.setInt("slotsize", layout.slotSize)
    }

    Using.resource(TableScan(tx, "fldcat", fieldCatalogLayout)) { ts =>
      schema.fields.foreach { fieldName =>
        ts.insert()
        ts.setString("tblname", tableName)
        ts.setString("fldname", fieldName)
        ts.setInt("type", schema.`type`(fieldName).value)
        ts.setInt("length", schema.length(fieldName))
        ts.setInt("offset", layout.offset(fieldName))
      }
    }
