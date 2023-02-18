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

  def xLock(blockId: BlockId): Unit =
    locks.get(blockId) match
      case None | Some(S) =>
        sLock(blockId)
        lockTable.xLock(blockId)
        locks.put(blockId, X)
      case Some(X) => ()

  def sLock(blockId: BlockId): Unit =
    locks.get(blockId) match
      case None =>
        lockTable.sLock(blockId)
        locks.put(blockId, S)
      case Some(_lockType) => ()
      
  def release(): Unit =
    locks.keySet.foreach(blockId => lockTable.unLock(blockId))
    locks.clear()

object ConcurrencyManager:
  val lockTable = new LockTable()
