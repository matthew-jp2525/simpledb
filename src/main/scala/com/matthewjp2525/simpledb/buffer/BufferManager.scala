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

  def flushAll(transactionNumber: TransactionNumber): Unit =
    bufferPool.foreach { buffer =>
      if buffer.modifyingTransactions == transactionNumber then
        buffer.flush()
    }

  def unpin(buffer: Buffer): Unit = synchronized {
    buffer.unpin()
    if !buffer.isPinned then
      availableNumber += 1
      notifyAll()
  }

  def pin(blockId: BlockId, waitMaxTime: Long = MAX_TIME): Buffer = synchronized {
    val startTime = System.currentTimeMillis()

    @tailrec
    def retryToPin(blockId: BlockId): Buffer =
      val result = tryToPin(blockId)
      result match
        case None =>
          if waitingTooLong(startTime, waitMaxTime) then
            throw BufferAbortException
          else
            Try {
              wait(waitMaxTime)
            } match
              case Success(()) =>
                retryToPin(blockId)
              case Failure(_e: InterruptedException) => throw BufferAbortException
              case Failure(e) => throw e

        case Some(aBuffer) => aBuffer

    retryToPin(blockId)
  }

  private def waitingTooLong(startTime: Long, waitMaxTime: Long): Boolean =
    System.currentTimeMillis() - startTime > waitMaxTime

  private def tryToPin(blockId: BlockId): Option[Buffer] =
    val maybeExistingBuffer = findExistingBuffer(blockId)

    val maybeBuffer =
      maybeExistingBuffer match
        case None =>
          val maybeUnpinnedBuffer = chooseUnpinnedBuffer
          maybeUnpinnedBuffer match
            case None => None
            case Some(unpinnedBuffer) =>
              unpinnedBuffer.assignToBlock(blockId)
              Some(unpinnedBuffer)
        case Some(existingBuffer) => Some(existingBuffer)

    maybeBuffer match
      case None => None
      case Some(aBuffer) =>
        if !aBuffer.isPinned then
          availableNumber -= 1
        aBuffer.pin()
        Some(aBuffer)


  private def findExistingBuffer(blockId: BlockId): Option[Buffer] =
    bufferPool.find(buffer => {
      val maybeBlockId = buffer.block
      maybeBlockId match
        case None => false
        case Some(aBlockId) =>
          aBlockId equals blockId
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

