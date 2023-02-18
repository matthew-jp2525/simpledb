package com.matthewjp2525.simpledb.buffer

import com.matthewjp2525.simpledb.buffer.BufferException.BufferAbortException
import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.LogManager
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.util.Failure

class BufferManagerTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("pin request is aborted when no available buffers exist") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile")
    val bufferManager = BufferManager(fileManager, logManager, 3)
    assertEquals(bufferManager.available, 3)

    val buffers = new Array[Buffer](6)

    buffers(0) = bufferManager.pin(BlockId("testfile", 0), 1)
    assertEquals(bufferManager.available, 2)

    buffers(1) = bufferManager.pin(BlockId("testfile", 1), 1)
    assertEquals(bufferManager.available, 1)

    buffers(2) = bufferManager.pin(BlockId("testfile", 2), 1)
    assertEquals(bufferManager.available, 0)

    bufferManager.unpin(buffers(1))
    buffers(1) = null
    assertEquals(bufferManager.available, 1)

    buffers(3) = bufferManager.pin(BlockId("testfile", 0), 1)
    assertEquals(bufferManager.available, 1)

    buffers(4) = bufferManager.pin(BlockId("testfile", 1), 1)
    assertEquals(bufferManager.available, 0)

    intercept[BufferAbortException.type] {
      bufferManager.pin(BlockId("testfile", 3), 1)
    }

    bufferManager.unpin(buffers(2))
    buffers(2) = null
    assertEquals(bufferManager.available, 1)

    buffers(5) = bufferManager.pin(BlockId("testfile", 3), 1)
    assertEquals(bufferManager.available, 0)

    val resultingBufferAssignment = buffers.zipWithIndex.map((buffer, i) => (Option(buffer).flatMap(_.block), i)).toList
    assertEquals(resultingBufferAssignment, List(
      (Some(BlockId("testfile", 0)), 0),
      (None, 1),
      (None, 2),
      (Some(BlockId("testfile", 0)), 3),
      (Some(BlockId("testfile", 1)), 4),
      (Some(BlockId("testfile", 3)), 5)
    ))
  }