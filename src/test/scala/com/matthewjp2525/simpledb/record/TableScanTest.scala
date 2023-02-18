package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.buffer.BufferManager
import com.matthewjp2525.simpledb.filemanager.FileManager
import com.matthewjp2525.simpledb.log.LogManager
import com.matthewjp2525.simpledb.record.FieldType.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.transaction.{Transaction, TransactionNumberGenerator}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class TableScanTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("table scan test") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile")
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

    val tableScan = TableScan(tx, "T", layout)

    @tailrec
    def fillTableWithRecords(number: Int = 0): Unit =
      if number < 20 then
        tableScan.insert()
        tableScan.setInt("A", number)
        tableScan.setString("B", "rec" + number)
        fillTableWithRecords(number + 1)

    tableScan.beforeFirst()
    fillTableWithRecords()

    @tailrec
    def collectRecords(acc: ListBuffer[(Int, String)] = ListBuffer.empty[(Int, String)]): List[(Int, String)] =
      val next = tableScan.next()
      if next then
        val a = tableScan.getInt("A")
        val b = tableScan.getString("B")
        // val rid = tableScan.getRid
        acc += Tuple2(a, b)
        collectRecords(acc)
      else
        acc.toList

    tableScan.beforeFirst()
    val records = collectRecords()

    assertEquals(records, List(
      (0, "rec0"),
      (1, "rec1"),
      (2, "rec2"),
      (3, "rec3"),
      (4, "rec4"),
      (5, "rec5"),
      (6, "rec6"),
      (7, "rec7"),
      (8, "rec8"),
      (9, "rec9"),
      (10, "rec10"),
      (11, "rec11"),
      (12, "rec12"),
      (13, "rec13"),
      (14, "rec14"),
      (15, "rec15"),
      (16, "rec16"),
      (17, "rec17"),
      (18, "rec18"),
      (19, "rec19")
    ))

    @tailrec
    def deleteSubset(): Unit =
      val next = tableScan.next()
      if next then
        val a = tableScan.getInt("A")
        if a % 3 == 0 then
          tableScan.delete()
        deleteSubset()

    tableScan.beforeFirst()
    deleteSubset()

    tableScan.beforeFirst()
    val remainingRecords = collectRecords()

    assertEquals(remainingRecords, List(
      (1, "rec1"),
      (2, "rec2"),
      (4, "rec4"),
      (5, "rec5"),
      (7, "rec7"),
      (8, "rec8"),
      (10, "rec10"),
      (11, "rec11"),
      (13, "rec13"),
      (14, "rec14"),
      (16, "rec16"),
      (17, "rec17"),
      (19, "rec19")
    ))

    tableScan.close()
    tx.commit()
  }