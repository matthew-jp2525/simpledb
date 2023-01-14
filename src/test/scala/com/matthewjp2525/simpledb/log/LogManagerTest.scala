package com.matthewjp2525.simpledb.log

import com.matthewjp2525.simpledb.filemanager.{FileManager, Page}
import org.apache.commons.io.FileUtils

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.file.Files

class LogManagerTest extends munit.FunSuite {
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("can read and write logs") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile").get

    (1 to 35).foreach { i =>
      val string = s"record${i}"
      val number = i + 100
      val numberPosition = Page.maxLength(string.length)
      val record = new Array[Byte](numberPosition + Integer.BYTES)
      val page = Page(record)
      page.setString(0, string)
      page.setInt(numberPosition, number)
      logManager.append(record)
    }

    val result =
      logManager.iterator.get.map { record =>
        val page = Page(record)
        val string = page.getString(0)
        val numberPosition = Page.maxLength(string.length)
        val number = page.getInt(numberPosition)

        (string, number)
      }.toList

    assertEquals(result, List(
      ("record35", 135),
      ("record34", 134),
      ("record33", 133),
      ("record32", 132),
      ("record31", 131),
      ("record30", 130),
      ("record29", 129),
      ("record28", 128),
      ("record27", 127),
      ("record26", 126),
      ("record25", 125),
      ("record24", 124),
      ("record23", 123),
      ("record22", 122),
      ("record21", 121),
      ("record20", 120),
      ("record19", 119),
      ("record18", 118),
      ("record17", 117),
      ("record16", 116),
      ("record15", 115),
      ("record14", 114),
      ("record13", 113),
      ("record12", 112),
      ("record11", 111),
      ("record10", 110),
      ("record9", 109),
      ("record8", 108),
      ("record7", 107),
      ("record6", 106),
      ("record5", 105),
      ("record4", 104),
      ("record3", 103),
      ("record2", 102),
      ("record1", 101)
    ))
  }
}
