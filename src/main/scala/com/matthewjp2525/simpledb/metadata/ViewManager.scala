package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.metadata.ViewManagerException.ViewDefNotFoundException
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.{Schema, TableScan}
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class ViewManagerException extends Exception with Product with Serializable

object ViewManagerException:
  case class ViewDefNotFoundException(viewName: String) extends ViewManagerException:
    override def getMessage: String = s"specified view ($viewName) not found | " + super.getMessage

class ViewManager private(tableManager: TableManager):
  def createView(viewName: String, viewDef: String, tx: Transaction): Try[Unit] =
    for layout <- tableManager.getLayout("viewcat", tx)
        ts <- TableScan(tx, "viewcat", layout)
        _ <- ts.setString("viewname", viewName)
        _ <- ts.setString("viewdef", viewDef)
    yield ts.close()

  def getViewDef(viewName: String, tx: Transaction): Try[String] =
    @tailrec
    def lookupViewDef(ts: TableScan): Try[String] =
      ts.next() match
        case Success(true) =>
          ts.getString("viewname") match
            case Success(aViewName) =>
              if aViewName equals viewName then
                ts.getString("viewdef")
              else
                lookupViewDef(ts)
            case Failure(e) => Failure(e)
        case Success(false) =>
          Failure(ViewDefNotFoundException(viewName))
        case Failure(e) => Failure(e)

    for layout <- tableManager.getLayout("viewcat", tx)
        ts <- TableScan(tx, "viewcat", layout)
        viewDef <- lookupViewDef(ts)
    yield {
      ts.close()
      viewDef
    }

object ViewManager:
  private val MAX_VIEWDEF = 100

  def apply(isNew: Boolean, tableManager: TableManager, tx: Transaction): Try[ViewManager] =
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
      ).map(_ => new ViewManager(tableManager))
    else
      Success(new ViewManager(tableManager))
