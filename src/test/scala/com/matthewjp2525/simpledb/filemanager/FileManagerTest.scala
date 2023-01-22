package com.matthewjp2525.simpledb.filemanager

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

class FileManagerTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("can read and write files") { testDir =>
    val fileManager = FileManager(testDir, 400)

    val blockId = BlockId("testfile", 2)
    val page1 = Page(fileManager.blockSize)
    val position1 = 88
    page1.setString(position1, "あいうえお")
    val size = Page.maxLength("あいうえお".length)
    val position2 = position1 + size
    page1.setInt(position2, 345)
    fileManager.write(blockId, page1)

    val page2 = Page(fileManager.blockSize)
    fileManager.read(blockId, page2)

    assertEquals(page2.getInt(position2), 345)
    assertEquals(page2.getString(position1), "あいうえお")
  }
