package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.metadata.StatManager.{calcTableStats, refreshStatistics}
import com.matthewjp2525.simpledb.record.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec
import scala.util.{Try, Using}

case class StatInfo(numBlocks: Int, numRecords: Int):
  def blocksAccessed: Int = numBlocks

  def recordsOutput: Int = numRecords

  // TODO
  def distinctValues(fieldName: FieldName): Int =
    1 + (numRecords / 3)

class StatManager private(private val tableManager: TableManager):
  private var numCalls: Int = 0
  private var tableStats: Map[TableName, StatInfo] = Map.empty[TableName, StatInfo]

  def getStatInfo(tableName: TableName, layout: Layout, tx: Transaction): StatInfo = synchronized {
    numCalls += 1

    if numCalls > 100 then
      refreshStatistics(this, tx)

    val maybeStatInfo = tableStats.get(tableName)

    maybeStatInfo match
      case None =>
        val statInfo = calcTableStats(tableName, layout, tx)
        tableStats += (tableName -> statInfo)
        statInfo
      case Some(statInfo) => statInfo
  }


object StatManager:
  def apply(tableManager: TableManager, tx: Transaction): StatManager =
    val statManager = new StatManager(tableManager)
    refreshStatistics(statManager, tx)
    statManager

  private def refreshStatistics(statManager: StatManager, tx: Transaction): Unit = synchronized {
    statManager.tableStats = Map.empty[TableName, StatInfo]
    statManager.numCalls = 0
    val layout = statManager.tableManager.getLayout("tblcat", tx)

    @tailrec
    def go(ts: TableScan): Unit =
      if ts.next() then
        val tableName = ts.getString("tblname")
        val layout = statManager.tableManager.getLayout("tblname", tx)
        val statInfo = calcTableStats(tableName, layout, tx)
        statManager.tableStats = statManager.tableStats + (tableName -> statInfo)
        go(ts)

    Using.resource(TableScan(tx, "tblcat", layout))(go)
  }

  private def calcTableStats(tableName: TableName, layout: Layout, tx: Transaction): StatInfo =
    @tailrec
    def go(
            ts: TableScan,
            accNumRecords: Int,
            accNumBlocks: Int
          ): (Int, Int) =
      if ts.next() then
        go(
          ts,
          accNumRecords + 1,
          ts.getRid.blockNumber + 1
        )
      else
        (accNumRecords, accNumBlocks)

    val (numRecords, numBlocks) =
      Using.resource(TableScan(tx, tableName, layout))(go(_, 0, 0))

    StatInfo(numBlocks, numRecords)
