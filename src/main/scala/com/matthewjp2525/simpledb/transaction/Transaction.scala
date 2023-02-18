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
  def commit(): Unit =
    recoveryManager.commit(this)
    concurrencyManager.release()
    myBuffers.unpinAll()
    println(s"transaction $transactionNumber committed")

  def rollback(): Unit =
    recoveryManager.rollback(this)
    concurrencyManager.release()
    myBuffers.unpinAll()
    println(s"transaction $transactionNumber rolled back")

  def recover(): Unit =
    bufferManager.flushAll(transactionNumber)
    recoveryManager.recover(this)

  /* methods to access buffers */
  def pin(blockId: BlockId): Unit = myBuffers.pin(blockId)

  def unpin(blockId: BlockId): Unit = myBuffers.unpin(blockId)

  def getInt(blockId: BlockId, offset: Int): Int =
    concurrencyManager.sLock(blockId)
    val buffer = myBuffers.getBuffer(blockId).getOrElse(throw MissingBufferException)
    buffer.contents.getInt(offset)

  def getString(blockId: BlockId, offset: Int): String =
    concurrencyManager.sLock(blockId)
    val buffer = myBuffers.getBuffer(blockId).getOrElse(throw MissingBufferException)
    buffer.contents.getString(offset)

  def setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean): Unit =
    concurrencyManager.xLock(blockId)
    val buffer = myBuffers.getBuffer(blockId).getOrElse(throw MissingBufferException)
    val logSequenceNumber =
      if okToLog then
        recoveryManager.setInt(this, buffer, offset)
      else
        -1

    buffer.contents.setInt(offset, value)
    buffer.setModified(transactionNumber, logSequenceNumber)

  def setString(blockId: BlockId, offset: Int, value: String, okToLog: Boolean): Unit =
    concurrencyManager.xLock(blockId)
    val buffer = myBuffers.getBuffer(blockId).getOrElse(throw MissingBufferException)
    val logSequenceNumber =
      if okToLog then
        recoveryManager.setString(this, buffer, offset)
      else
        -1

    buffer.contents.setString(offset, value)
    buffer.setModified(transactionNumber, logSequenceNumber)

  def availableBuffers: Int = bufferManager.available

  /* methods related to the file manager */
  def size(fileName: String): Int =
    val dummyBlockId = BlockId(fileName, END_OF_FILE)
    concurrencyManager.sLock(dummyBlockId)
    fileManager.length(fileName)

  def append(fileName: String): BlockId =
    val dummyBlockId = BlockId(fileName, END_OF_FILE)
    concurrencyManager.xLock(dummyBlockId)
    fileManager.append(fileName)

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