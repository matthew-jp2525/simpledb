package com.matthewjp2525.simpledb.metadata

import com.matthewjp2525.simpledb.buffer.BufferManager
import com.matthewjp2525.simpledb.filemanager.FileManager
import com.matthewjp2525.simpledb.log.LogManager
import com.matthewjp2525.simpledb.record.*
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.FieldType.*
import com.matthewjp2525.simpledb.transaction.{Transaction, TransactionNumberGenerator}
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

class TableManagerTest extends munit.FunSuite:
  val testDirs: FunFixture[File] = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(test.name).toFile
    },
    teardown = { testDir =>
      FileUtils.deleteDirectory(testDir)
    }
  )

  testDirs.test("table manager test") { testDir =>
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

    val layout = tableManager.getLayout("my_table", tx)

    tx.commit()

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
  }
