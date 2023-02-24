package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.BlockId
import com.matthewjp2525.simpledb.record.LayoutOps.*
import com.matthewjp2525.simpledb.record.RecordPage.{EMPTY, USED}
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec

type Slot = Int
type Flag = Int

class RecordPage private(tx: Transaction, val blockId: BlockId, layout: Layout):
  def getInt(slot: Slot, fieldName: FieldName): Int =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.getInt(blockId, fieldPosition)

  def getString(slot: Slot, fieldName: FieldName): String =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.getString(blockId, fieldPosition)

  def setInt(slot: Slot, fieldName: FieldName, value: Int): Unit =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.setInt(blockId, fieldPosition, value, true)

  def setString(slot: Slot, fieldName: FieldName, value: String): Unit =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.setString(blockId, fieldPosition, value, true)

  private def offset(slot: Slot): Offset = slot * layout.slotSize

  def delete(slot: Slot): Unit =
    setFlag(slot, EMPTY)

  private def setFlag(slot: Slot, flag: Flag): Unit =
    tx.setInt(blockId, offset(slot), flag, true)

  def format(): Unit =
    @tailrec
    def doFormat(slot: Slot): Unit =
      if isValidSlot(slot) then
        tx.setInt(blockId, offset(slot), EMPTY, false)
        layout.schema.fields.foreach { fieldName =>
          val fieldPosition = offset(slot) + layout.offset(fieldName)
          layout.schema.`type`(fieldName) match
            case FieldType.Integer =>
              tx.setInt(blockId, fieldPosition, 0, false)
            case FieldType.Varchar =>
              tx.setString(blockId, fieldPosition, "", false)
        }
        doFormat(slot + 1)
      else
        ()

    doFormat(0)

  private def isValidSlot(slot: Slot): Boolean =
    offset(slot + 1) <= tx.blockSize

  def nextAfter(slot: Slot): Slot =
    searchAfter(slot, USED)

  def insertAfter(slot: Slot): Slot =
    val newSlot = searchAfter(slot, EMPTY)
    if newSlot >= 0 then
      setFlag(newSlot, USED)
    newSlot

  private def searchAfter(slot: Slot, flag: Flag): Slot =
    @tailrec
    def doSearchAfter(slot: Slot): Slot =
      val nextSlot: Slot = slot + 1
      if isValidSlot(nextSlot) then
        val aFlag = tx.getInt(blockId, offset(nextSlot))
        if aFlag == flag then
          nextSlot
        else
          doSearchAfter(nextSlot)
      else
        -1

    doSearchAfter(slot)

object RecordPage:
  val EMPTY: Int = 0
  val USED: Int = 1

  def apply(tx: Transaction, blockId: BlockId, layout: Layout): RecordPage =
    tx.pin(blockId)
    new RecordPage(tx, blockId, layout)