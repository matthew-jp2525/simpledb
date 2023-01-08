package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.{BlockId, BlockNumber, FileName}
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
                       ):
  private var currentRecordPage: Option[RecordPage] = None
  private var currentSlot: Slot = -1

  def hasField(fieldName: FieldName): Boolean =
    layout.schema.hasField(fileName)

  // Methods that establish the current record
  def beforeFirst(): Try[Unit] = moveToBlock(this, 0)

  def next(): Try[Boolean] =
    @tailrec
    def go(recordPage: RecordPage): Try[Boolean] =
      if currentSlot < 0 then
        atLastBlock(recordPage, tx, fileName) match
          case Failure(e) => Failure(e)
          case Success(true) => Success(false)
          case Success(false) =>
            (for _ <- moveToBlock(this, recordPage.blockId.blockNumber + 1)
                 slot <- recordPage.nextAfter(currentSlot)
            yield slot) match
              case Failure(e) => Failure(e)
              case Success(slot) =>
                currentSlot = slot
                go(recordPage)
      else
        Success(true)

    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.nextAfter(currentSlot) match
      case Failure(e) => Failure(e)
      case Success(slot) =>
        currentSlot = slot
        go(recordPage)

  def moveToRid(rid: RID): Try[Unit] =
    close()
    val blockId = BlockId(fileName, rid.blockNumber)
    RecordPage(tx, blockId, layout).map { recordPage =>
      currentRecordPage = Some(recordPage)
    }

  def close(): Unit =
    currentRecordPage.fold(()) { aRecordPage =>
      tx.unpin(aRecordPage.blockId)
    }

  def getRid: RID =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    RID(recordPage.blockId.blockNumber, currentSlot)

  def insert(): Try[Unit] =
    @tailrec
    def go(recordPage: RecordPage): Try[Unit] =
      if currentSlot < 0 then
        atLastBlock(recordPage, tx, fileName) match
          case Failure(e) => Failure(e)
          case Success(result) =>
            (
              for _ <- {
                if result then
                  moveToNewBlock(this)
                else
                  moveToBlock(this, recordPage.blockId.blockNumber + 1)
              }
                  slot <- recordPage.insertAfter(currentSlot)
              yield slot) match
              case Failure(e) => Failure(e)
              case Success(slot) =>
                currentSlot = slot
                go(recordPage)
      else
        Success(())

    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.insertAfter(currentSlot) match
      case Failure(e) => Failure(e)
      case Success(slot) =>
        currentSlot = slot
        go(recordPage)

  // Methods that access the current record
  def getInt(fieldName: FieldName): Try[Int] =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.getInt(currentSlot, fieldName)

  def getString(fieldName: FieldName): Try[String] =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.getString(currentSlot, fieldName)

  def setInt(fieldName: FieldName, value: Int): Try[Unit] =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.setInt(currentSlot, fieldName, value)

  def setString(fieldName: FieldName, value: String): Try[Unit] =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.setString(currentSlot, fieldName, value)

  def delete: Try[Unit] =
    val recordPage = currentRecordPage.getOrElse(throw MissingRecordPageException)
    recordPage.delete(currentSlot)

object TableScan:
  def apply(tx: Transaction, tableName: TableName, layout: Layout): Try[TableScan] =
    val fileName = tableName + ".tbl"
    val tableScan = new TableScan(tx, layout, fileName)

    for size <- tx.size(fileName)
        _ <- {
          if size == 0 then
            moveToNewBlock(tableScan)
          else
            moveToBlock(tableScan, 0)
        }
    yield tableScan

  private def moveToBlock(tableScan: TableScan, blockNumber: BlockNumber): Try[Unit] =
    tableScan.close()

    val blockId = BlockId(tableScan.fileName, blockNumber)

    for recordPage <- RecordPage(tableScan.tx, blockId, tableScan.layout)
        _ = tableScan.currentRecordPage = Some(recordPage)
    yield ()

  private def moveToNewBlock(tableScan: TableScan): Try[Unit] =
    tableScan.close()

    for blockId <- tableScan.tx.append(tableScan.fileName)
        recordPage <- RecordPage(tableScan.tx, blockId, tableScan.layout)
        _ = tableScan.currentRecordPage = Some(recordPage)
    yield ()

  private def atLastBlock(recordPage: RecordPage, tx: Transaction, fileName: FileName): Try[Boolean] =
    tx.size(fileName).map { size =>
      size == recordPage.blockId.blockNumber
    }
