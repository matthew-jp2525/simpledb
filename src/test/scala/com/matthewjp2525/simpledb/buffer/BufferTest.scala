package com.matthewjp2525.simpledb.buffer

import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.LogManager
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

class BufferTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("modified buffer gets written to disk when replaced") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile").get
    val bufferManager = BufferManager(fileManager, logManager, 3)

    val buffer1 = bufferManager.pin(BlockId("testfile", 1)).get
    val number = buffer1.contents.getInt(80)
    buffer1.contents.setInt(80, number + 1)
    buffer1.setModified(1, 0)
    println(s"The new value is ${number + 1}")
    bufferManager.unpin(buffer1)

    bufferManager.pin(BlockId("testfile", 2))

    val page = Page(fileManager.blockSize)
    fileManager.read(BlockId("testfile", 1), page)

    assertEquals(page.getInt(80), 1)
  }
