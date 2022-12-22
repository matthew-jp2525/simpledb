package com.matthewjp2525.simpledb.transaction.recovery

import com.matthewjp2525.simpledb.buffer.{Buffer, BufferManager, TransactionNumber}
import com.matthewjp2525.simpledb.log.{LSN, LogManager}
import com.matthewjp2525.simpledb.transaction.Transaction
import com.matthewjp2525.simpledb.transaction.recovery.LogRecord.*
import com.matthewjp2525.simpledb.transaction.recovery.RecoveryManagerException.MissingBlockException

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class RecoveryManagerException extends Exception with Product with Serializable

object RecoveryManagerException:
  case object MissingBlockException extends RecoveryManagerException

class RecoveryManager(logManager: LogManager, bufferManager: BufferManager):
  def commit(tx: Transaction): Try[Unit] =
    for _ <- bufferManager.flushAll(tx.transactionNumber)
        logSequenceNumber <- CommitRecord(tx.transactionNumber).writeToLog(logManager)
        _ <- logManager.flush(logSequenceNumber)
    yield ()

  def rollback(tx: Transaction): Try[Unit] =
    for _ <- doRollback(tx)
        _ <- bufferManager.flushAll(tx.transactionNumber)
        logSequenceNumber <- RollbackRecord(tx.transactionNumber).writeToLog(logManager)
        _ <- logManager.flush(logSequenceNumber)
    yield ()

  private def doRollback(tx: Transaction): Try[Unit] =
    @tailrec
    def go(iterator: Iterator[Array[Byte]]): Try[Unit] =
      iterator.nextOption match
        case None => Success(())
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
                      case StartRecord(_aTransactionNumber) => Success(())
                      case _ =>
                        logRecord.undo(tx) match
                          case Success(()) => go(iterator)
                          // Stop rollback on failure.
                          case Failure(e) => Failure(e)
                  else
                    go(iterator)

    logManager.iterator match
      case Success(iterator) => go(iterator)
      case Failure(e) => Failure(e)

  def recover(tx: Transaction): Try[Unit] =
    for _ <- doRecover(tx)
        _ <- bufferManager.flushAll(tx.transactionNumber)
        logSequenceNumber <- CheckpointRecord.writeToLog(logManager)
        _ <- logManager.flush(logSequenceNumber)
    yield ()

  private def doRecover(tx: Transaction): Try[Unit] =
    @tailrec
    def go(iterator: Iterator[Array[Byte]], finishedTransactionNumbers: List[Int]): Try[Unit] =
      iterator.nextOption match
        case None => Success(())
        case Some(bytes) =>
          LogRecord(bytes) match
            // Skip unknown operation.
            case None =>
              go(iterator, finishedTransactionNumbers)
            case Some(CheckpointRecord) =>
              Success(())
            case Some(CommitRecord(aTransactionNumber)) =>
              go(iterator, aTransactionNumber :: finishedTransactionNumbers)
            case Some(RollbackRecord(aTransactionNumber)) =>
              go(iterator, aTransactionNumber :: finishedTransactionNumbers)
            case Some(logRecord) =>
              // All record type except checkpoint has transaction number.
              val aTransactionNumber = logRecord.maybeTransactionNumber.get
              if !finishedTransactionNumbers.contains(aTransactionNumber) then
                logRecord.undo(tx) match
                  case Success(()) => go(iterator, finishedTransactionNumbers)
                  case Failure(e) => Failure(e)
              else
                go(iterator, finishedTransactionNumbers)

    logManager.iterator match
      case Success(iterator) => go(iterator, List.empty[Int])
      case Failure(e) => Failure(e)

  def setInt(tx: Transaction, buffer: Buffer, offset: Int): Try[LSN] =
    val oldValue = buffer.contents.getInt(offset)
    buffer.block match
      case None => Failure(MissingBlockException)
      case Some(blockId) =>
        SetIntRecord(tx.transactionNumber, offset, oldValue, blockId).writeToLog(logManager)

  def setString(tx: Transaction, buffer: Buffer, offset: Int): Try[LSN] =
    val oldValue = buffer.contents.getString(offset)
    buffer.block match
      case None => Failure(MissingBlockException)
      case Some(blockId) =>
        SetStringRecord(tx.transactionNumber, offset, oldValue, blockId).writeToLog(logManager)
