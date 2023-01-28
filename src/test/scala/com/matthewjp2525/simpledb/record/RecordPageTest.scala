package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.buffer.BufferManager
import com.matthewjp2525.simpledb.filemanager.FileManager
import com.matthewjp2525.simpledb.log.LogManager
import com.matthewjp2525.simpledb.record.FieldType.*
import com.matthewjp2525.simpledb.transaction.{Transaction, TransactionNumberGenerator}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class RecordPageTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("record page test") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile").get
    val bufferManager = BufferManager(fileManager, logManager, 8)
    val transactionNumberGenerator = new TransactionNumberGenerator()
    val tx = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)

    val schema = Schema()
      .addIntField("A")
      .addStringField("B", 9)

    val layout = Layout(schema)

    assertEquals(layout, Layout(
      Schema(
        Vector("A", "B"),
        Map(
          "A" -> FieldInfo(Integer, 0),
          "B" -> FieldInfo(Varchar, 9)
        )
      ),
      Map("A" -> 4, "B" -> 8),
      39
    ))

    val blockId = tx.append("testfile").get
    tx.pin(blockId)
    val recordPage = RecordPage(tx, blockId, layout).get
    recordPage.format().get

    @tailrec
    def fillPageWithRecords(slot: Slot = -1, number: Int = 100): Unit =
      val aSlot = recordPage.insertAfter(slot).get
      if aSlot >= 0 then
        recordPage.setInt(aSlot, "A", number).get
        recordPage.setString(aSlot, "B", "rec" + number).get
        fillPageWithRecords(aSlot, number + 1)

    fillPageWithRecords()

    @tailrec
    def collectContents(slot: Slot = -1, acc: ListBuffer[(Slot, Int, String)] = ListBuffer.empty[(Slot, Int, String)]): List[(Slot, Int, String)] =
      val aSlot = recordPage.nextAfter(slot).get
      if aSlot >= 0 then
        val a = recordPage.getInt(aSlot, "A").get
        val b = recordPage.getString(aSlot, "B").get
        acc += Tuple3(aSlot, a, b)
        collectContents(aSlot, acc)
      else
        acc.toList

    val contents = collectContents()

    assertEquals(contents, List(
      (0, 100, "rec100"),
      (1, 101, "rec101"),
      (2, 102, "rec102"),
      (3, 103, "rec103"),
      (4, 104, "rec104"),
      (5, 105, "rec105"),
      (6, 106, "rec106"),
      (7, 107, "rec107"),
      (8, 108, "rec108"),
      (9, 109, "rec109")
    ))

    @tailrec
    def deleteSubset(slot: Slot): Unit =
      val aSlot = recordPage.nextAfter(slot).get
      if aSlot >= 0 then
        val a = recordPage.getInt(aSlot, "A").get
        if a % 3 == 0 then
          recordPage.delete(aSlot).get
        deleteSubset(aSlot)

    deleteSubset(-1)

    val remainingContents = collectContents(-1)

    assertEquals(remainingContents, List(
      (0, 100, "rec100"),
      (1, 101, "rec101"),
      (3, 103, "rec103"),
      (4, 104, "rec104"),
      (6, 106, "rec106"),
      (7, 107, "rec107"),
      (9, 109, "rec109")
    ))

    tx.unpin(blockId)
    tx.commit().get
  }