package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.buffer.BufferManager
import com.matthewjp2525.simpledb.filemanager.FileManager
import com.matthewjp2525.simpledb.log.LogManager
import com.matthewjp2525.simpledb.record.*
import com.matthewjp2525.simpledb.record.FieldType.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.transaction.{Transaction, TransactionNumberGenerator}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

class CatalogTableTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("catalog table test") { testDir =>
    val fileManager = FileManager(testDir, 400)
    val logManager = LogManager(fileManager, "testlogfile")
    val bufferManager = BufferManager(fileManager, logManager, 8)
    val transactionNumberGenerator = new TransactionNumberGenerator()
    val tx = Transaction(fileManager, logManager, bufferManager, transactionNumberGenerator)

    val tableManager = TableManager(true, tx)

    val schema = Schema()
      .addIntField("A")
      .addStringField("B", 9)

    TableManager.createTable(
      "my_table",
      schema,
      tableManager.tableCatalogLayout,
      tableManager.fieldCatalogLayout,
      tx
    )

    val tableCatalogLayout = tableManager.getLayout("tblcat", tx)

    val tcat = TableScan(tx, "tblcat", tableCatalogLayout)

    @tailrec
    def readTableCatalog(
                          acc: ListBuffer[(TableName, SlotSize)] = ListBuffer.empty[(TableName, SlotSize)]
                        ): List[(TableName, SlotSize)] =
      if tcat.next() then
        val tableName = tcat.getString("tblname")
        val slotSize = tcat.getInt("slotsize")
        readTableCatalog(acc += Tuple2(tableName, slotSize))
      else
        acc.toList

    val tableCatalog = readTableCatalog()

    tcat.close()


    val fieldCatalogLayout = tableManager.getLayout("fldcat", tx)

    val fcat = TableScan(tx, "fldcat", fieldCatalogLayout)

    @tailrec
    def readFieldCatalog(
                          acc: ListBuffer[(TableName, FieldName, Offset)] = ListBuffer.empty[(TableName, FieldName, Offset)]
                        ): List[(TableName, FieldName, Offset)] =
      if fcat.next() then
        val tableName = fcat.getString("tblname")
        val fieldName = fcat.getString("fldname")
        val offset = fcat.getInt("offset")
        readFieldCatalog(acc += Tuple3(tableName, fieldName, offset))
      else
        acc.toList

    val fieldCatalog = readFieldCatalog()

    fcat.close()

    tx.commit()

    assertEquals(tableCatalog.map(x => x._1), List(
      "tblcat",
      "fldcat",
      "my_table"
    ))

    assertEquals(fieldCatalog.map(x => (x._1, x._2)), List(
      ("tblcat", "tblname"),
      ("tblcat", "slotsize"),
      ("fldcat", "tblname"),
      ("fldcat", "fldname"),
      ("fldcat", "type"),
      ("fldcat", "length"),
      ("fldcat", "offset"),
      ("my_table", "A"),
      ("my_table", "B")
    ))
  }
