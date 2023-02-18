package com.matthewjp2525.simpledb.buffer

import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.{LSN, LogManager}

import scala.util.{Success, Try}

type TransactionNumber = Int

class Buffer private(fileManager: FileManager, logManager: LogManager, val contents: Page):
  private var maybeBlockId: Option[BlockId] = None
  private var transactionNumber: TransactionNumber = -1
  private var logSequenceNumber: LSN = -1
  private var pins: Int = 0

  def setModified(transactionNumber: TransactionNumber, logSequenceNumber: LSN): Unit =
    this.transactionNumber = transactionNumber
    if (logSequenceNumber >= 0) this.logSequenceNumber = logSequenceNumber

  def isPinned: Boolean = pins > 0

  def modifyingTransactions: TransactionNumber = transactionNumber

  def assignToBlock(blockId: BlockId): Unit =
    flush()
    maybeBlockId = Some(blockId)
    fileManager.read(blockId, contents)
    pins = 0

  def flush(): Unit =
    block.fold(()) { blockId =>
      if transactionNumber >= 0 then
        logManager.flush(logSequenceNumber)
        fileManager.write(blockId, contents)
        transactionNumber = -1
    }

  def block: Option[BlockId] = maybeBlockId

  def pin(): Unit = pins += 1

  def unpin(): Unit = pins -= 1

object Buffer:
  def apply(fileManager: FileManager, logManager: LogManager): Buffer =
    new Buffer(
      fileManager = fileManager,
      logManager = logManager,
      contents = Page(fileManager.blockSize)
    )
