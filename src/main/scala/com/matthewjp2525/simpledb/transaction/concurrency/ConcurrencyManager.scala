package com.matthewjp2525.simpledb.transaction.concurrency

import com.matthewjp2525.simpledb.filemanager.BlockId
import com.matthewjp2525.simpledb.transaction.concurrency.ConcurrencyManager.lockTable
import com.matthewjp2525.simpledb.transaction.concurrency.LockType.{S, X}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

enum LockType:
  case S extends LockType
  case X extends LockType

class ConcurrencyManager:
  private val locks = new mutable.HashMap[BlockId, LockType]()

  def xLock(blockId: BlockId): Try[Unit] =
    locks.get(blockId) match
      case None | Some(S) =>
        for _ <- sLock(blockId)
            _ <- lockTable.xLock(blockId)
            _ = locks.put(blockId, X)
        yield ()
      case Some(X) => Success(())

  def sLock(blockId: BlockId): Try[Unit] =
    locks.get(blockId) match
      case None =>
        for _ <- lockTable.sLock(blockId)
            _ = locks.put(blockId, S)
        yield ()
      case Some(_lockType) => Success(())
      
  def release(): Unit =
    locks.keySet.foreach(blockId => lockTable.unLock(blockId))
    locks.clear()

object ConcurrencyManager:
  val lockTable = new LockTable()
