package com.matthewjp2525.simpledb.log

import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.LogIterator.moveToBlock

import scala.util.{Failure, Success, Try}

class LogIterator private(
                           val fileManager: FileManager,
                           var blockId: BlockId,
                           val page: Page,
                         ) extends Iterator[Array[Byte]]:
  private var currentPosition: Int = 0
  private var boundary: Int = 0

  override def next: Array[Byte] =
    if currentPosition == fileManager.blockSize then
      val destBlockId = BlockId(blockId.fileName, blockId.blockNumber - 1)
      blockId = destBlockId
      moveToBlock(this, destBlockId).map { _ =>
        val record = page.getBytes(currentPosition)
        currentPosition += Integer.BYTES + record.length
        record
      }.get
    else
      val record = page.getBytes(currentPosition)
      currentPosition += Integer.BYTES + record.length
      record

  override def hasNext: Boolean =
    currentPosition < fileManager.blockSize || blockId.blockNumber > 0

object LogIterator:
  def apply(fileManager: FileManager, blockId: BlockId): Try[LogIterator] =
    val logIterator = new LogIterator(
      fileManager = fileManager,
      blockId = blockId,
      page = Page(new Array[Byte](fileManager.blockSize))
    )
    moveToBlock(logIterator, blockId)

  def moveToBlock(logIterator: LogIterator, blockId: BlockId): Try[LogIterator] =
    logIterator.fileManager.read(blockId, logIterator.page).map { _ =>
      logIterator.boundary = logIterator.page.getInt(0)
      logIterator.currentPosition = logIterator.boundary
      logIterator
    }