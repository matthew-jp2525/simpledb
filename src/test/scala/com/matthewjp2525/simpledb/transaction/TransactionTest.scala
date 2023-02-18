package com.matthewjp2525.simpledb.transaction

import com.matthewjp2525.simpledb.buffer.BufferException.BufferAbortException
import com.matthewjp2525.simpledb.buffer.BufferManager
import com.matthewjp2525.simpledb.filemanager.{BlockId, FileManager, Page}
import com.matthewjp2525.simpledb.log.LogManager
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.util.Failure

class TransactionTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("commit and rollback work") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile")
    val bufferManager = BufferManager(fileManager, logManager, 8)
    val transactionNumberGenerator = new TransactionNumberGenerator()
    val blockId = BlockId("testfile", 1)

    val tx1 = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)
    tx1.pin(blockId)
    tx1.setInt(blockId, 80, 1, false)
    tx1.setString(blockId, 40, "one", false)
    tx1.commit()

    val tx2 = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)
    tx2.pin(blockId)
    val intValue = tx2.getInt(blockId, 80)
    val stringValue = tx2.getString(blockId, 40)
    assertEquals(intValue, 1)
    assertEquals(stringValue, "one")
    val newIntValue = intValue + 1
    val newStringValue = stringValue + "!"
    tx2.setInt(blockId, 80, newIntValue, true)
    tx2.setString(blockId, 40, newStringValue, true)
    tx2.commit()

    val tx3 = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)
    tx3.pin(blockId)
    assertEquals(tx3.getInt(blockId, 80), 2)
    assertEquals(tx3.getString(blockId, 40), "one!")
    tx3.setInt(blockId, 80, 9999, true)
    assertEquals(tx3.getInt(blockId, 80), 9999)
    tx3.rollback()

    val tx4 = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)
    tx4.pin(blockId)
    assertEquals(tx4.getInt(blockId, 80), 2)
    tx4.commit()
  }