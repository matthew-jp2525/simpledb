package com.matthewjp2525.simpledb.buffer

import com.matthewjp2525.simpledb.buffer.BufferException.BufferAbortException
import com.matthewjp2525.simpledb.buffer.BufferManager.MAX_TIME
import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager}
import com.matthewjp2525.simpledb.log.LogManager

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

sealed abstract class BufferException extends Exception with Product with Serializable

object BufferException:
   case object BufferAbortException extends BufferException

class BufferManager private(bufferPool: Array[Buffer], private var availableNumber: Int):
  type StartTime = Long

  def available: Int = synchronized {
    availableNumber
  }

  def flushAll(transactionNumber: TransactionNumber): Try[Unit] =
    Try {
      for buffer <- bufferPool
          if buffer.modifyingTransactions == transactionNumber
      yield buffer.flush.get
    }

  def unpin(buffer: Buffer): Unit = synchronized {
    buffer.unpin()
    if !buffer.isPinned then
      availableNumber += 1
      notifyAll()
  }

  def pin(blockId: BlockId): Try[Buffer] = synchronized {
    given StartTime = System.currentTimeMillis()

    retryToPin(blockId)
  }

  private def waitingTooLong(using startTime: StartTime): Boolean =
    System.currentTimeMillis() - startTime > MAX_TIME

  @tailrec
  private def retryToPin(blockId: BlockId)(using startTime: StartTime): Try[Buffer] =
    val result = tryToPin(blockId)
    result match
      case Success(None) =>
        if waitingTooLong then
          Failure(BufferAbortException)
        else
          wait(MAX_TIME)
          retryToPin(blockId)
      case Success(Some(buffer)) => Success(buffer)
      case Failure(e) => Failure(e)

  private def tryToPin(blockId: BlockId): Try[Option[Buffer]] =
    val maybeExistingBuffer = findExistingBuffer(blockId)

    val maybeBufferOrFailure =
      maybeExistingBuffer match
        case None =>
          val maybeUnpinnedBuffer = chooseUnpinnedBuffer
          maybeUnpinnedBuffer match
            case None => Success(None)
            case Some(unpinnedBuffer) =>
              unpinnedBuffer.assignToBlock(blockId).map(_ => Some(unpinnedBuffer))
        case Some(existingBuffer) => Success(Some(existingBuffer))

    maybeBufferOrFailure match
      case Failure(e) => Failure(e)
      case Success(None) => Success(None)
      case Success(Some(aBuffer)) =>
        if (!aBuffer.isPinned) then
          availableNumber -= 1
        aBuffer.pin()
        Success(Some(aBuffer))


  private def findExistingBuffer(blockId: BlockId): Option[Buffer] =
    bufferPool.find(buffer => {
      val maybeBlockId = buffer.block
      maybeBlockId match
        case None => false
        case Some(aBlockId) =>
          aBlockId equals BlockId
    })

  private def chooseUnpinnedBuffer: Option[Buffer] =
    bufferPool.find(buffer => !buffer.isPinned)


object BufferManager:
  val MAX_TIME = 10000

  def apply(fileManager: FileManager, logManager: LogManager, numberOfBuffers: Int): BufferManager =
    new BufferManager(
      bufferPool = new Array[Buffer](numberOfBuffers).map(_ => Buffer(fileManager, logManager)),
      availableNumber = numberOfBuffers
    )

