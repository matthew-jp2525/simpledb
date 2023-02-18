package com.matthewjp2525.simpledb.transaction.recovery

import com.matthewjp2525.simpledb.buffer.{Buffer, BufferManager, TransactionNumber}
import com.matthewjp2525.simpledb.log.{LSN, LogManager}
import com.matthewjp2525.simpledb.transaction.Transaction
import com.matthewjp2525.simpledb.transaction.recovery.LogRecord.*
import com.matthewjp2525.simpledb.transaction.recovery.LogRecordOps.*
import com.matthewjp2525.simpledb.transaction.recovery.RecoveryManagerException.MissingBlockException

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class RecoveryManagerException extends Exception with Product with Serializable

object RecoveryManagerException:
  case object MissingBlockException extends RecoveryManagerException

class RecoveryManager(logManager: LogManager, bufferManager: BufferManager):
  def commit(tx: Transaction): Unit =
    bufferManager.flushAll(tx.transactionNumber)
    val logSequenceNumber = CommitRecord(tx.transactionNumber).writeToLog(logManager)
    logManager.flush(logSequenceNumber)

  def rollback(tx: Transaction): Unit =
    doRollback(tx)
    bufferManager.flushAll(tx.transactionNumber)
    val logSequenceNumber = RollbackRecord(tx.transactionNumber).writeToLog(logManager)
    logManager.flush(logSequenceNumber)

  private def doRollback(tx: Transaction): Unit =
    @tailrec
    def go(iterator: Iterator[Array[Byte]]): Unit =
      iterator.nextOption match
        case None => ()
        case Some(bytes) =>
          LogRecord(bytes) match
            // Skip unknown operation.
            case None =>
              go(iterator)
            case Some(logRecord) =>
              logRecord.maybeTransactionNumber match
                case None =>
                  go(iterator)
                case Some(aTransactionNumber) =>
                  if aTransactionNumber == tx.transactionNumber then
                    logRecord match
                      case StartRecord(_aTransactionNumber) => ()
                      case _ =>
                        logRecord.undo(tx)
                        go(iterator)
                  else
                    go(iterator)

    go(logManager.iterator)

  def recover(tx: Transaction): Unit =
    doRecover(tx)
    bufferManager.flushAll(tx.transactionNumber)
    val logSequenceNumber = CheckpointRecord.writeToLog(logManager)
    logManager.flush(logSequenceNumber)

  private def doRecover(tx: Transaction): Unit =
    @tailrec
    def go(iterator: Iterator[Array[Byte]], finishedTransactionNumbers: List[Int]): Unit =
      iterator.nextOption match
        case None => ()
        case Some(bytes) =>
          LogRecord(bytes) match
            // Skip unknown operation.
            case None =>
              go(iterator, finishedTransactionNumbers)
            case Some(CheckpointRecord) =>
              ()
            case Some(CommitRecord(aTransactionNumber)) =>
              go(iterator, aTransactionNumber :: finishedTransactionNumbers)
            case Some(RollbackRecord(aTransactionNumber)) =>
              go(iterator, aTransactionNumber :: finishedTransactionNumbers)
            case Some(logRecord) =>
              // All record type except checkpoint has transaction number.
              val aTransactionNumber = logRecord.maybeTransactionNumber.get
              if !finishedTransactionNumbers.contains(aTransactionNumber) then
                logRecord.undo(tx)
              go(iterator, finishedTransactionNumbers)

    go(logManager.iterator, List.empty[Int])

  def setInt(tx: Transaction, buffer: Buffer, offset: Int): LSN =
    val oldValue = buffer.contents.getInt(offset)
    val blockId = buffer.block.getOrElse(throw MissingBlockException)
    SetIntRecord(tx.transactionNumber, offset, oldValue, blockId).writeToLog(logManager)

  def setString(tx: Transaction, buffer: Buffer, offset: Int): LSN =
    val oldValue = buffer.contents.getString(offset)
    val blockId = buffer.block.getOrElse(throw MissingBlockException)
    SetStringRecord(tx.transactionNumber, offset, oldValue, blockId).writeToLog(logManager)
