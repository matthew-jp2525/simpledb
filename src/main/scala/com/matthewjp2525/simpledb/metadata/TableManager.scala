package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.metadata.TableManager.MAX_NAME
import com.matthewjp2525.simpledb.metadata.TableManagerException.TableNotFoundException
import com.matthewjp2525.simpledb.record.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.LayoutOps.*
import com.matthewjp2525.simpledb.transaction.Transaction
import com.matthewjp2525.simpledb.util.SeqSupport.foreachTry

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class TableManagerException extends Exception with Product with Serializable

object TableManagerException:
  case class TableNotFoundException(tableName: TableName) extends TableManagerException:
    override def getMessage: String = s"specified table ($tableName) not found | " + super.getMessage

class TableManager private(
                            val tableCatalogLayout: Layout,
                            val fieldCatalogLayout: Layout
                          ):

  def getLayout(tableName: TableName, tx: Transaction): Try[Layout] =
    @tailrec
    def lookupTableCatalog(ts: TableScan): Try[SlotSize] =
      ts.next() match
        case Success(true) =>
          ts.getString("tblname") match
            case Success(aTableName) =>
              if aTableName == tableName then
                ts.getInt("slotsize")
              else
                lookupTableCatalog(ts)
            case Failure(e) => Failure(e)
        case Success(false) =>
          Failure(TableNotFoundException(tableName))
        case Failure(e) => Failure(e)

    @tailrec
    def lookupFieldCatalog(
                            ts: TableScan,
                            accSchema: Schema,
                            accOffsets: Map[FieldName, Offset]
                          ): Try[(Schema, Map[FieldName, Offset])] =
      ts.next() match
        case Success(true) =>
          ts.getString("tblname") match
            case Success(aTableName) =>
              if aTableName == tableName then
                (for fieldName <- ts.getString("fldname")
                     fieldTypeValue <- ts.getInt("type")
                     fieldType = FieldType.fromValue(fieldTypeValue)
                     fieldLength <- ts.getInt("length")
                     offset <- ts.getInt("offset")
                yield (fieldName, fieldType, fieldLength, offset)) match
                  case Success((fieldName, fieldType, fieldLength, offset)) =>
                    lookupFieldCatalog(
                      ts,
                      accSchema.addField(fieldName, fieldType, fieldLength),
                      accOffsets + (fieldName -> offset)
                    )
                  case Failure(e) => Failure(e)
              else
                lookupFieldCatalog(
                  ts,
                  accSchema,
                  accOffsets
                )
            case Failure(e) => Failure(e)
        case Success(false) =>
          Success(accSchema, accOffsets)
        case Failure(e) => Failure(e)

    for tcat <- TableScan(tx, "tblcat", tableCatalogLayout)
        slotSize <- lookupTableCatalog(tcat)
        _ = tcat.close()
        fcat <- TableScan(tx, "fldcat", fieldCatalogLayout)
        (schema, offsets) <- lookupFieldCatalog(fcat, Schema(), Map.empty[FieldName, Offset])
        _ = fcat.close()
    yield Layout(schema, offsets, slotSize)

object TableManager:
  val MAX_NAME = 16

  def apply(isNew: Boolean, tx: Transaction): Try[TableManager] =
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
      for _ <- createTable(
        "tblcat",
        tableCatalogSchema,
        tableCatalogLayout,
        fieldCatalogLayout,
        tx
      )
          _ <- createTable(
            "fldcat",
            fieldCatalogSchema,
            tableCatalogLayout,
            fieldCatalogLayout,
            tx
          )
      yield new TableManager(tableCatalogLayout, fieldCatalogLayout)
    else
      Success(new TableManager(tableCatalogLayout, fieldCatalogLayout))

  def createTable(
                   tableName: TableName,
                   schema: Schema,
                   tableCatalogLayout: Layout,
                   fieldCatalogLayout: Layout,
                   tx: Transaction
                 ): Try[Unit] =
    val layout = Layout(schema)

    def createTableCatalog(): Try[Unit] =
      for ts <- TableScan(tx, "tblcat", tableCatalogLayout)
          _ <- ts.insert()
          _ <- ts.setString("tblname", tableName)
          _ <- ts.setInt("slotsize", layout.slotSize)
      yield ts.close()

    def createFieldCatalog(): Try[Unit] =
      for ts <- TableScan(tx, "fldcat", fieldCatalogLayout)
          _ <- foreachTry(schema.fields) { fieldName =>
            for _ <- ts.insert()
                _ <- ts.setString("tblname", tableName)
                _ <- ts.setString("fldname", fieldName)
                _ <- ts.setInt("type", schema.`type`(fieldName).value)
                _ <- ts.setInt("length", schema.length(fieldName))
                _ <- ts.setInt("offset", layout.offset(fieldName))
            yield ()
          }
      yield ts.close()

    for _ <- createTableCatalog()
        _ <- createFieldCatalog()
    yield ()
