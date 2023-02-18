package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.{BlockId, BlockNumber, FileName}
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.TableScan.{atLastBlock, moveToBlock, moveToNewBlock}
import com.matthewjp2525.simpledb.record.TableScanException.MissingRecordPageException
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class TableScanException extends Exception with Product with Serializable

object TableScanException:
  case object MissingRecordPageException extends TableScanException

type TableName = String

class TableScan private(
                         val tx: Transaction,
                         val layout: Layout,
                         val fileName: FileName
                       ) extends AutoCloseable:
  private var currentRecordPage: Option[RecordPage] = None
  private var currentSlot: Slot = -1

  def hasField(fieldName: FieldName): Boolean =
    layout.schema.hasField(fileName)

  // Methods that establish the current record
  def beforeFirst(): Unit = moveToBlock(this, 0)

  def next(): Boolean =
    @tailrec
    def go(): Boolean =
      if currentSlot < 0 then
        if atLastBlock(
          currentRecordPage.getOrElse(throw MissingRecordPageException),
          tx,
          fileName
        ) then
          false
        else
          moveToBlock(
            this,
            currentRecordPage.getOrElse(throw MissingRecordPageException).blockId.blockNumber + 1
          )
          val slot = currentRecordPage.getOrElse(throw MissingRecordPageException).nextAfter(currentSlot)
          currentSlot = slot
          go()
      else
        true

    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    val slot = recordPage.nextAfter(currentSlot)
    currentSlot = slot
    go()

  def moveToRid(rid: RID): Unit =
    close()
    val blockId = BlockId(fileName, rid.blockNumber)
    val recordPage = RecordPage(tx, blockId, layout)
    currentRecordPage = Some(recordPage)

  def close(): Unit =
    currentRecordPage.fold(()) { aRecordPage =>
      tx.unpin(aRecordPage.blockId)
    }

  def getRid: RID =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    RID(recordPage.blockId.blockNumber, currentSlot)

  def insert(): Unit =
    @tailrec
    def go(): Unit =
      if currentSlot < 0 then
        if atLastBlock(
          currentRecordPage.getOrElse(throw MissingRecordPageException),
          tx,
          fileName
        ) then
          moveToNewBlock(this)
        else
          moveToBlock(
            this,
            currentRecordPage.getOrElse(throw MissingRecordPageException).blockId.blockNumber + 1
          )
          
        val slot = currentRecordPage.getOrElse(throw MissingRecordPageException).insertAfter(currentSlot)
        currentSlot = slot
        go()

    val slot = currentRecordPage.getOrElse(throw MissingRecordPageException).insertAfter(currentSlot)
    currentSlot = slot
    go()

  // Methods that access the current record
  def getInt(fieldName: FieldName): Int =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.getInt(currentSlot, fieldName)

  def getString(fieldName: FieldName): String =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.getString(currentSlot, fieldName)

  def setInt(fieldName: FieldName, value: Int): Unit =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.setInt(currentSlot, fieldName, value)

  def setString(fieldName: FieldName, value: String): Unit =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.setString(currentSlot, fieldName, value)

  def delete(): Unit =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.delete(currentSlot)

object TableScan:
  def apply(tx: Transaction, tableName: TableName, layout: Layout): TableScan =
    val fileName = tableName + ".tbl"
    val tableScan = new TableScan(tx, layout, fileName)

    val size = tx.size(fileName)
    if size == 0 then
      moveToNewBlock(tableScan)
    else
      moveToBlock(tableScan, 0)
      
    tableScan

  private def moveToBlock(tableScan: TableScan, blockNumber: BlockNumber): Unit =
    tableScan.close()
    val blockId = BlockId(tableScan.fileName, blockNumber)
    val recordPage = RecordPage(tableScan.tx, blockId, tableScan.layout)
    tableScan.currentRecordPage = Some(recordPage)
    tableScan.currentSlot = -1

  private def moveToNewBlock(tableScan: TableScan): Unit =
    tableScan.close()
    val blockId = tableScan.tx.append(tableScan.fileName)
    val recordPage = RecordPage(tableScan.tx, blockId, tableScan.layout)
    tableScan.currentRecordPage = Some(recordPage)
    recordPage.format()
    tableScan.currentSlot = -1

  private def atLastBlock(recordPage: RecordPage, tx: Transaction, fileName: FileName): Boolean =
    val size = tx.size(fileName)
    recordPage.blockId.blockNumber == size - 1
