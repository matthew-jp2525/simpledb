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
  def read(blockId: BlockId, page: Page): Try[Unit] = synchronized {
    val result =
      for file <- getFile(blockId.fileName)
          _ <- Try {
            file.seek(blockId.blockNumber * blockSize)
          }
          _ <- Try {
            file.getChannel.read(page.contents)
          }
      yield ()

    result match
      case Success(()) => Success(())
      case Failure(e) => Failure(new RuntimeException("cannot read block " + blockId + " | " + e.getMessage))
  }

  private def getFile(fileName: String): Try[RandomAccessFile] =
    Try {
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
    }

  def write(blockId: BlockId, page: Page): Try[Unit] = synchronized {
    val result =
      for file <- getFile(blockId.fileName)
          _ <- Try {
            file.seek(blockId.blockNumber * blockSize)
          }
          _ <- Try {
            file.getChannel.write(page.contents)
          }
      yield ()

    result match
      case Success(()) => Success(())
      case Failure(e) => Failure(new RuntimeException("cannot write block " + blockId + " | " + e.getMessage))
  }

  def append(fileName: String): Try[BlockId] = synchronized {
    val result =
      for
        newBlockNumber <- length(fileName)
        newBlockId = BlockId(fileName, newBlockNumber)
        file <- getFile(newBlockId.fileName)
        _ <- Try {
          file.seek(newBlockId.blockNumber)
        }
        byteArray = new Array[Byte](blockSize)
        _ <- Try {
          file.write(byteArray)
        }
      yield newBlockId

    result match
      case Success(aNewBlockId) => Success(aNewBlockId)
      case Failure(e) => Failure(new RuntimeException("cannot append block " + " | " + e.getMessage))
  }

  def length(fileName: String): Try[Int] =
    val result =
      for file <- getFile(fileName)
          fileLength <- Try {
            file.length()
          }
      yield (fileLength / blockSize).toInt

    result match
      case Success(aLength) => Success(aLength)
      case Failure(e) => Failure(new RuntimeException("cannot access " + fileName + " | " + e.getMessage))

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
