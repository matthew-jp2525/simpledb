package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.record.{Layout, Schema, TableName}
import com.matthewjp2525.simpledb.transaction.Transaction

// TODO Manage index metadata.
class MetadataManager private (
                              tableManager: TableManager,
                              viewManager: ViewManager,
                              statManager: StatManager
                              ):
  def createTable(tableName: TableName, schema: Schema, tx: Transaction): Unit =
    tableManager.createTable(tableName, schema, tx)

  def getLayout(tableName: TableName, tx: Transaction): Layout =
    tableManager.getLayout(tableName, tx)

  def createView(viewName: String, viewDef: String, tx: Transaction): Unit =
    viewManager.createView(viewName, viewDef, tx)

  def getViewDef(viewName: String, tx: Transaction): String =
    viewManager.getViewDef(viewName, tx)

  def getStatInfo(tableName: TableName, layout: Layout, tx: Transaction): StatInfo =
    statManager.getStatInfo(tableName, layout, tx)

object MetadataManager:
  def apply(isNew: Boolean, tx: Transaction): MetadataManager =
    val tableManager = TableManager(isNew, tx)
    val viewManager = ViewManager(isNew, tableManager, tx)
    val statManager = StatManager(tableManager, tx)
    new MetadataManager(tableManager, viewManager, statManager)
