package com.matthewjp2525.simpledb.transaction.recovery

import com.matthewjp2525.simpledb.filemanager.{BlockId, Page}
import com.matthewjp2525.simpledb.log.{LSN, LogManager}
import com.matthewjp2525.simpledb.transaction.Transaction
import com.matthewjp2525.simpledb.transaction.recovery.LogRecord.*
import com.matthewjp2525.simpledb.transaction.recovery.Operation.*

import scala.util.{Success, Try}

enum Operation(val value: Int):
  case Checkpoint extends Operation(0)
  case Start extends Operation(1)
  case Commit extends Operation(2)
  case Rollback extends Operation(3)
  case SetInt extends Operation(4)
  case SetString extends Operation(5)

enum LogRecord(operation: Operation):
  case CheckpointRecord extends LogRecord(Checkpoint)
  case StartRecord(transactionNumber: Int) extends LogRecord(Start)
  case CommitRecord(transactionNumber: Int) extends LogRecord(Commit)
  case RollbackRecord(transactionNumber: Int) extends LogRecord(Rollback)
  case SetIntRecord(
                     transactionNumber: Int,
                     offset: Int,
                     oldValue: Int,
                     blockId: BlockId
                   ) extends LogRecord(SetInt)
  case SetStringRecord(
                        transactionNumber: Int,
                        offset: Int,
                        oldValue: String,
                        blockId: BlockId
                      ) extends LogRecord(SetString)

object LogRecord:
  def apply(byteArray: Array[Byte]): Option[LogRecord] =
    val page = Page(byteArray)
    val operation = page.getInt(0)
    val transactionPosition = Integer.BYTES
    val transactionNumber = page.getInt(transactionPosition)
    operation match
      case Checkpoint.value => Some(CheckpointRecord)
      case Start.value => Some(StartRecord(transactionNumber))
      case Commit.value => Some(CommitRecord(transactionNumber))
      case Rollback.value => Some(RollbackRecord(transactionNumber))
      case SetInt.value | SetString.value =>
        val fileNamePosition = transactionPosition + Integer.BYTES
        val fileName = page.getString(fileNamePosition)
        val blockNumberPosition = fileNamePosition + Page.maxLength(fileName.length)
        val blockNumber = page.getInt(blockNumberPosition)
        val blockId = BlockId(fileName, blockNumber)
        val offsetPosition = blockNumberPosition + Integer.BYTES
        val offset = page.getInt(offsetPosition)
        val valuePosition = offsetPosition + Integer.BYTES
        operation match
          case SetInt.value =>
            val value = page.getInt(valuePosition)
            Some(SetIntRecord(
              transactionNumber = transactionNumber,
              offset = offset,
              oldValue = value,
              blockId = blockId
            ))
          case SetString.value =>
            val value = page.getString(valuePosition)
            Some(SetStringRecord(
              transactionNumber = transactionNumber,
              offset = offset,
              oldValue = value,
              blockId = blockId
            ))
      case _ => None


object LogRecordOps:
  extension (logRecord: LogRecord)
    def maybeTransactionNumber: Option[Int] =
      logRecord match
        case CheckpointRecord => None
        case StartRecord(transactionNumber) => Some(transactionNumber)
        case CommitRecord(transactionNumber) => Some(transactionNumber)
        case RollbackRecord(transactionNumber) => Some(transactionNumber)
        case SetIntRecord(transactionNumber, _, _, _) => Some(transactionNumber)
        case SetStringRecord(transactionNumber, _, _, _) => Some(transactionNumber)

    def show: String =
      logRecord match
        case CheckpointRecord => "<CHECKPOINT>"
        case StartRecord(transactionNumber) => s"<START $transactionNumber>"
        case CommitRecord(transactionNumber) => s"<COMMIT $transactionNumber>"
        case RollbackRecord(transactionNumber) => s"<ROLLBACK $transactionNumber>"
        case SetIntRecord(transactionNumber, offset, oldValue, blockId) => s"<SETINT $transactionNumber $blockId $offset $oldValue>"
        case SetStringRecord(transactionNumber, offset, oldValue, blockId) => s"<SETSTRING $transactionNumber $blockId $offset $oldValue>"

    def undo(tx: Transaction): Try[Unit] =
      logRecord match
        case SetIntRecord(_transactionNumber, offset, oldValue, blockId) =>
          for _ <- tx.pin(blockId)
              _ <- tx.setInt(blockId, offset, oldValue, false)
              _ = tx.unpin(blockId)
          yield ()
        case SetStringRecord(_transactionNumber, offset, oldValue, blockId) =>
          for _ <- tx.pin(blockId)
              _ <- tx.setString(blockId, offset, oldValue, false)
              _ = tx.unpin(blockId)
          yield ()
        case _ => Success(())

    def writeToLog(logManager: LogManager): Try[LSN] =
      logRecord match
        case CheckpointRecord =>
          val recordLength = Integer.BYTES
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, Checkpoint.value)
          logManager.append(record)
        case StartRecord(transactionNumber) =>
          val transactionPosition = Integer.BYTES
          val recordLength = transactionPosition + Integer.BYTES
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, Start.value)
          page.setInt(transactionPosition, transactionNumber)
          logManager.append(record)
        case CommitRecord(transactionNumber) =>
          val transactionPosition = Integer.BYTES
          val recordLength = transactionPosition + Integer.BYTES
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, Commit.value)
          page.setInt(transactionPosition, transactionNumber)
          logManager.append(record)
        case RollbackRecord(transactionNumber) =>
          val transactionPosition = Integer.BYTES
          val recordLength = transactionPosition + Integer.BYTES
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, Rollback.value)
          page.setInt(transactionPosition, transactionNumber)
          logManager.append(record)
        case SetIntRecord(transactionNumber, offset, oldValue, blockId) =>
          val transactionPosition = Integer.BYTES
          val fileNamePosition = transactionPosition + Integer.BYTES
          val blockNumberPosition = fileNamePosition + Page.maxLength(blockId.fileName.length)
          val offsetPosition = blockNumberPosition + Integer.BYTES
          val valuePosition = offsetPosition + Integer.BYTES
          val recordLength = valuePosition + Integer.BYTES
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, SetInt.value)
          page.setInt(transactionPosition, transactionNumber)
          page.setString(fileNamePosition, blockId.fileName)
          page.setInt(blockNumberPosition, blockId.blockNumber)
          page.setInt(offsetPosition, offset)
          page.setInt(valuePosition, oldValue)
          logManager.append(record)
        case SetStringRecord(transactionNumber, offset, oldValue, blockId) =>
          val transactionPosition = Integer.BYTES
          val fileNamePosition = transactionPosition + Integer.BYTES
          val blockNumberPosition = fileNamePosition + Page.maxLength(blockId.fileName.length)
          val offsetPosition = blockNumberPosition + Integer.BYTES
          val valuePosition = offsetPosition + Integer.BYTES
          val recordLength = valuePosition + Page.maxLength(oldValue.length)
          val record = new Array[Byte](recordLength)
          val page = Page(record)
          page.setInt(0, SetString.value)
          page.setInt(transactionPosition, transactionNumber)
          page.setString(fileNamePosition, blockId.fileName)
          page.setInt(blockNumberPosition, blockId.blockNumber)
          page.setInt(offsetPosition, offset)
          page.setString(valuePosition, oldValue)
          logManager.append(record)

