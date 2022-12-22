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
    val startTime = System.currentTimeMillis()

    @tailrec
    def retryToPin(blockId: BlockId): Try[Buffer] =
      val result = tryToPin(blockId)
      result match
        case Success(None) =>
          if waitingTooLong(startTime) then
            Failure(BufferAbortException)
          else
            Try {
              wait(MAX_TIME)
            } match
              case Success(()) =>
                retryToPin(blockId)
              case Failure(_e: InterruptedException) => Failure(BufferAbortException)
              case Failure(e) => Failure(e)

        case Success(Some(buffer)) => Success(buffer)
        case Failure(e) => Failure(e)

    retryToPin(blockId)
  }

  private def waitingTooLong(startTime: Long): Boolean =
    System.currentTimeMillis() - startTime > MAX_TIME

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

