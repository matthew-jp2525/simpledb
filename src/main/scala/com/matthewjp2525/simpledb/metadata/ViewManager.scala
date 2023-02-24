package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.metadata.ViewManagerException.ViewDefNotFoundException
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.{Schema, TableScan}
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec
import scala.util.Using

sealed abstract class ViewManagerException extends Exception with Product with Serializable

object ViewManagerException:
  case class ViewDefNotFoundException(viewName: String) extends ViewManagerException:
    override def getMessage: String = s"specified view ($viewName) not found | " + super.getMessage

class ViewManager private(tableManager: TableManager):
  def createView(viewName: String, viewDef: String, tx: Transaction): Unit =
    val layout = tableManager.getLayout("viewcat", tx)
    Using.resource(TableScan(tx, "viewcat", layout)) { ts =>
      ts.setString("viewname", viewName)
      ts.setString("viewdef", viewDef)
    }

  def getViewDef(viewName: String, tx: Transaction): String =
    @tailrec
    def lookupViewDef(ts: TableScan): String =
      if ts.next() then
        val aViewName = ts.getString("viewname")
        if aViewName == viewName then
          ts.getString("viewdef")
        else
          lookupViewDef(ts)
      else
        throw ViewDefNotFoundException(viewName)

    val layout = tableManager.getLayout("viewcat", tx)
    Using.resource(TableScan(tx, "viewcat", layout)) { ts =>
      lookupViewDef(ts)
    }

object ViewManager:
  private val MAX_VIEWDEF = 100

  def apply(isNew: Boolean, tableManager: TableManager, tx: Transaction): ViewManager =
    if isNew then
      val schema = Schema()
        .addStringField("viewname", TableManager.MAX_NAME)
        .addStringField("viewdef", MAX_VIEWDEF)

      TableManager.createTable(
        "viewcat",
        schema,
        tableManager.tableCatalogLayout,
        tableManager.fieldCatalogLayout,
        tx
      )

    new ViewManager(tableManager)
