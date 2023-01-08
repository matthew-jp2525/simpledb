package com.matthewjp2525.simpledb.log

import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.LogManager.appendNewBlock

import scala.util.{Failure, Success, Try}

/**
 * `LSN` (or log sequence number), a log record's identifier.
 */
type LSN = Int

class LogManager private(
                          fileManager: FileManager,
                          logFileName: String,
                          logPage: Page,
                          private var currentBlockId: BlockId
                        ):
  private var latestLSN: LSN = 0
  private var lastSavedLSN: LSN = 0

  /**
   * places the log records in the page from right to left.
   */
  def append(logRecord: Array[Byte]): Try[LSN] = synchronized {
    /**
     * The variable boundary contains the offset of the most recently added record,
     * which is written to the first 4 bytes of the page.
     */
    val boundary = logPage.getInt(0)
    val recordSize = logRecord.length
    val bytesNeeded = recordSize + Integer.BYTES

    def doOperationsUnlessFit(): Try[Int] =

    /** If doesn't fit */
      if boundary - bytesNeeded < Integer.BYTES then

      /** so move to the next block */
        for _ <- flush
            newBlockId <- appendNewBlock(fileManager, logFileName, logPage)
        yield {
          currentBlockId = newBlockId

          /** the new boundary */
          logPage.getInt(0)
        }
      else Success(boundary)

    doOperationsUnlessFit().map { boundary =>
      val recordPosition = boundary - bytesNeeded
      logPage.setBytes(recordPosition, logRecord)

      /** the new boundary */
      logPage.setInt(0, recordPosition)

      latestLSN += 1
      latestLSN
    }
  }

  private def flush: Try[Unit] =
    for _ <- fileManager.write(currentBlockId, logPage)
      yield
        lastSavedLSN = latestLSN

  def flush(LSN: LSN): Try[Unit] =
    if LSN >= lastSavedLSN then
      flush
    else
      Success(())

  def iterator: Try[Iterator[Array[Byte]]] =
    for _ <- flush
        logIterator <- LogIterator(fileManager = fileManager, blockId = currentBlockId)
    yield logIterator

object LogManager:
  def apply(fileManager: FileManager, logFileName: String): Try[LogManager] =
    val logPage = Page(new Array[Byte](fileManager.blockSize))

    for logSize <- fileManager.length(logFileName)
        aBlockId <-
          if (logSize == 0) then
            appendNewBlock(fileManager, logFileName, logPage)
          else
            val aBlockId = BlockId(logFileName, logSize - 1)
            fileManager.read(aBlockId, logPage).map(_ => aBlockId)
    yield new LogManager(
      fileManager = fileManager,
      logFileName = logFileName,
      logPage = logPage,
      currentBlockId = aBlockId
    )

  private def appendNewBlock(fileManager: FileManager, logFileName: String, logPage: Page): Try[BlockId] =
    for blockId <- fileManager.append(logFileName)
        _ = logPage.setInt(0, fileManager.blockSize)
        _ <- fileManager.write(blockId, logPage)
    yield blockId
