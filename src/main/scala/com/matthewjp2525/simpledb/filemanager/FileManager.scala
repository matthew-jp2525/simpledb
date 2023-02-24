package com.matthewjp2525.simpledb.filemanager

import java.io.{File, RandomAccessFile}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class FileManager private(
                           dbDirectory: File,
                           val blockSize: Int,
                           isNew: Boolean,
                           openFiles: mutable.Map[String, RandomAccessFile]
                         ):
  def read(blockId: BlockId, page: Page): Unit = synchronized {
    Try {
      val file = getFile(blockId.fileName)
      file.seek(blockId.blockNumber * blockSize)
      file.getChannel.read(page.contents)
    } match
      case Success(_) => ()
      case Failure(e) => throw new RuntimeException("cannot read block " + blockId + " | " + e.getMessage)
  }

  private def getFile(fileName: String): RandomAccessFile =
    val maybeOpenFile = openFiles.get(fileName)
    maybeOpenFile match
      case Some(openFile) =>
        openFile
      case None =>
        val newFile = new RandomAccessFile(
          new File(dbDirectory, fileName),
          "rws"
        )
        openFiles.put(fileName, newFile)
        newFile

  def write(blockId: BlockId, page: Page): Unit = synchronized {
    Try {
      val file = getFile(blockId.fileName)
      file.seek(blockId.blockNumber * blockSize)
      file.getChannel.write(page.contents)
    } match
      case Success(_) => ()
      case Failure(e) => throw new RuntimeException("cannot write block " + blockId + " | " + e.getMessage)
  }

  def append(fileName: String): BlockId = synchronized {
    Try {
      val newBlockNumber = length(fileName)
      val newBlockId = BlockId(fileName, newBlockNumber)
      val file = getFile(newBlockId.fileName)
      file.seek(newBlockId.blockNumber * blockSize)
      val byteArray = new Array[Byte](blockSize)
      file.write(byteArray)

      newBlockId
    } match
      case Success(aNewBlockId) => aNewBlockId
      case Failure(e) => throw new RuntimeException("cannot append block " + " | " + e.getMessage)
  }

  def length(fileName: String): Int =
    Try {
      val file = getFile(fileName)
      val fileLength = file.length()
      (fileLength / blockSize).toInt
    } match
      case Success(aLength) => aLength
      case Failure(e) => throw new RuntimeException("cannot access " + fileName + " | " + e.getMessage)

object FileManager:
  def apply(dbDirectory: File, blockSize: Int): FileManager =
    val isNew = !dbDirectory.exists()

    if isNew then
      dbDirectory.mkdirs()

    for
      fileName <- dbDirectory.list()
      if fileName.startsWith("temp")
    do
      new File(dbDirectory, fileName).delete()

    new FileManager(
      dbDirectory = dbDirectory,
      blockSize = blockSize,
      isNew = isNew,
      openFiles = new mutable.HashMap[String, RandomAccessFile]()
    )
