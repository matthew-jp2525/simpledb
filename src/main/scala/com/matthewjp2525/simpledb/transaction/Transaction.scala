package com.matthewjp2525.simpledb.transaction

import com.matthewjp2525.simpledb.buffer.{Buffer, BufferManager}
import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager}
import com.matthewjp2525.simpledb.log.LogManager
import com.matthewjp2525.simpledb.transaction.Transaction.END_OF_FILE
import com.matthewjp2525.simpledb.transaction.TransactionException.MissingBufferException
import com.matthewjp2525.simpledb.transaction.concurrency.ConcurrencyManager
import com.matthewjp2525.simpledb.transaction.recovery.RecoveryManager

import scala.util.{Failure, Success, Try}

sealed abstract class TransactionException extends Exception with Product with Serializable

object TransactionException:
  case object MissingBufferException extends TransactionException

class Transaction private(
                           fileManager: FileManager,
                           bufferManager: BufferManager,
                           val transactionNumber: Int,
                           recoveryManager: RecoveryManager,
                           concurrencyManager: ConcurrencyManager,
                           myBuffers: BufferList
                         ):
  /* methods related to the transaction's lifespan */
  def commit(): Try[Unit] =
    recoveryManager.commit(this).map { _ =>
      concurrencyManager.release()
      myBuffers.unpinAll()
      println(s"transaction $transactionNumber committed")
    }

  def rollback(): Try[Unit] =
    recoveryManager.rollback(this).map { _ =>
      concurrencyManager.release()
      myBuffers.unpinAll()
      println(s"transaction $transactionNumber rolled back")
    }

  def recover(): Try[Unit] =
    for _ <- bufferManager.flushAll(transactionNumber)
        _ <- recoveryManager.recover(this)
    yield ()

  /* methods to access buffers */
  def pin(blockId: BlockId): Try[Unit] = myBuffers.pin(blockId)

  def unpin(blockId: BlockId): Unit = myBuffers.unpin(blockId)

  def getInt(blockId: BlockId, offset: Int): Try[Int] =
    concurrencyManager.sLock(blockId) match
      case Failure(e) => Failure(e)
      case Success(()) =>
        myBuffers.getBuffer(blockId) match
          case None => Failure(MissingBufferException)
          case Some(buffer) =>
            Success {
              buffer.contents.getInt(offset)
            }

  def getString(blockId: BlockId, offset: Int): Try[String] =
    concurrencyManager.sLock(blockId) match
      case Failure(e) => Failure(e)
      case Success(()) =>
        myBuffers.getBuffer(blockId) match
          case None => Failure(MissingBufferException)
          case Some(buffer) =>
            Success {
              buffer.contents.getString(offset)
            }

  def setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean): Try[Unit] =
    concurrencyManager.xLock(blockId).flatMap { _ =>
      myBuffers.getBuffer(blockId) match
        case None => Failure(MissingBufferException)
        case Some(buffer) =>
          val logSequenceNumberOrFailure =
            if okToLog then
              recoveryManager.setInt(this, buffer, offset)
            else
              Success(-1)

          logSequenceNumberOrFailure.map { logSequenceNumber =>
            buffer.contents.setInt(offset, value)
            buffer.setModified(transactionNumber, logSequenceNumber)
          }
    }

  def setString(blockId: BlockId, offset: Int, value: String, okToLog: Boolean): Try[Unit] =
    concurrencyManager.xLock(blockId).flatMap { _ =>
      myBuffers.getBuffer(blockId) match
        case None => Failure(MissingBufferException)
        case Some(buffer) =>
          val logSequenceNumberOrFailure =
            if okToLog then
              recoveryManager.setString(this, buffer, offset)
            else
              Success(-1)

          logSequenceNumberOrFailure.map { logSequenceNumber =>
            buffer.contents.setString(offset, value)
            buffer.setModified(transactionNumber, logSequenceNumber)
          }
    }

  def availableBuffers: Int = bufferManager.available

  /* methods related to the file manager */
  def size(fileName: String): Try[Int] =
    val dummyBlockId = BlockId(fileName, END_OF_FILE)
    for _ <- concurrencyManager.sLock(dummyBlockId)
        length <- fileManager.length(fileName)
    yield length

  def append(fileName: String): Try[BlockId] =
    val dummyBlockId = BlockId(fileName, END_OF_FILE)
    for _ <- concurrencyManager.xLock(dummyBlockId)
        blockId <- fileManager.append(fileName)
    yield blockId

  def blockSize: Int = fileManager.blockSize

object Transaction:
  val END_OF_FILE: Int = -1

  def apply(
             fileManager: FileManager,
             logManager: LogManager,
             bufferManager: BufferManager,
             transactionNumberGenerator: TransactionNumberGenerator // Shared by all transactions.
           ): Transaction =
    new Transaction(
      fileManager = fileManager,
      bufferManager = bufferManager,
      transactionNumber = transactionNumberGenerator.generate(),
      recoveryManager = new RecoveryManager(logManager, bufferManager),
      concurrencyManager = new ConcurrencyManager(),
      myBuffers = new BufferList(bufferManager)
    )

class TransactionNumberGenerator:
  private var value: Int = 0

  def generate(): Int = synchronized {
    value += 1
    value
  }