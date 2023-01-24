package com.matthewjp2525.simpledb.transaction.concurrency

import com.matthewjp2525.simpledb.filemanager.BlockId
import com.matthewjp2525.simpledb.transaction.concurrency.ConcurrencyException.LockAbortException
import com.matthewjp2525.simpledb.transaction.concurrency.Lock.{SLock, XLock}
import com.matthewjp2525.simpledb.transaction.concurrency.LockTable.{MAX_TIME, waitingTooLong}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

sealed abstract class ConcurrencyException extends Exception with Product with Serializable

object ConcurrencyException:
  case object LockAbortException extends ConcurrencyException

enum Lock:
  case XLock extends Lock
  case SLock(count: Int) extends Lock

class LockTable:
  private val locks = new mutable.HashMap[BlockId, Lock]()

  def sLock(blockId: BlockId): Try[Unit] = synchronized {
    val startTime = System.currentTimeMillis()

    @tailrec
    def retry(): Try[Unit] =
      (locks.get(blockId), waitingTooLong(startTime)) match
        case (Some(XLock), false) =>
          Try {
            wait(MAX_TIME)
          } match
            case Success(()) =>
              retry()
            case Failure(_e: InterruptedException) => Failure(LockAbortException)
            case Failure(e) => Failure(e)
        case (Some(XLock), true) =>
          Failure(LockAbortException)
        case (Some(SLock(count)), _) =>
          Success {
            locks.put(blockId, SLock(count + 1))
          }
        case (None, _) =>
          Success {
            locks.put(blockId, SLock(1))
          }

    retry()
  }

  def xLock(blockId: BlockId): Try[Unit] = synchronized {
    val startTime = System.currentTimeMillis()

    @tailrec
    def retry(): Try[Unit] =
    // Don't check xlock here because concurrency manager will always obtain an slock on the block before requesting the xlock.
      ((locks.get(blockId), waitingTooLong(startTime)): @unchecked) match
        // case when other slock exists
        case (Some(SLock(count)), false) if count > 1 =>
          Try {
            wait(MAX_TIME)
          } match
            case Success(()) =>
              retry()
            case Failure(_e: InterruptedException) => Failure(LockAbortException)
            case Failure(e) => Failure(e)
        // case when other slock exists and waiting too long
        case (Some(SLock(count)), true) if count > 1 =>
          Failure(LockAbortException)
        case _ =>
          Success {
            locks.put(blockId, XLock)
          }

    retry()
  }

  def unLock(blockId: BlockId): Unit =
    locks.get(blockId) match
      case Some(SLock(count)) =>
        if count > 1 then
          locks.put(blockId, SLock(count - 1))
        else
          locks.remove(blockId)
          notifyAll()
      case Some(XLock) =>
        locks.remove(blockId)
        notifyAll()
      case None => ()

object LockTable:
  private val MAX_TIME = 10000 // 10 seconds

  private def waitingTooLong(startTime: Long): Boolean =
    System.currentTimeMillis() - startTime > MAX_TIME

