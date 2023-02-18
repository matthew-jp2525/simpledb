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
  def append(logRecord: Array[Byte]): LSN = synchronized {
    /**
     * The variable boundary contains the offset of the most recently added record,
     * which is written to the first 4 bytes of the page.
     */
    val boundary = logPage.getInt(0)
    val recordSize = logRecord.length
    val bytesNeeded = recordSize + Integer.BYTES

    def doOperationsUnlessFit(): Int =

    /** If doesn't fit */
      if boundary - bytesNeeded < Integer.BYTES then
        /** so move to the next block */
        flush()
        val newBlockId = appendNewBlock(fileManager, logFileName, logPage)
        currentBlockId = newBlockId

        /** the new boundary */
        logPage.getInt(0)
      else
        boundary

    val newBoundary = doOperationsUnlessFit()
    val recordPosition = newBoundary - bytesNeeded
    logPage.setBytes(recordPosition, logRecord)

    /** the new boundary */
    logPage.setInt(0, recordPosition)

    latestLSN += 1
    latestLSN
  }

  def flush(logSequenceNumber: LSN): Unit =
    if logSequenceNumber >= lastSavedLSN then
      flush()

  def iterator: Iterator[Array[Byte]] =
    flush()
    LogIterator(fileManager = fileManager, blockId = currentBlockId)

  private def flush(): Unit =
    fileManager.write(currentBlockId, logPage)
    lastSavedLSN = latestLSN

object LogManager:
  def apply(fileManager: FileManager, logFileName: String): LogManager =
    val logPage = Page(new Array[Byte](fileManager.blockSize))
    val logSize = fileManager.length(logFileName)
    val aBlockId =
      if logSize == 0 then
        appendNewBlock(fileManager, logFileName, logPage)
      else
        val aBlockId = BlockId(logFileName, logSize - 1)
        fileManager.read(aBlockId, logPage)
        aBlockId

    new LogManager(
      fileManager = fileManager,
      logFileName = logFileName,
      logPage = logPage,
      currentBlockId = aBlockId
    )

  private def appendNewBlock(fileManager: FileManager, logFileName: String, logPage: Page): BlockId =
    val blockId = fileManager.append(logFileName)
    logPage.setInt(0, fileManager.blockSize)
    fileManager.write(blockId, logPage)
    blockId
