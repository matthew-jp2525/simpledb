package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.BlockId
import com.matthewjp2525.simpledb.record.RecordPage.{EMPTY, USED}
import com.matthewjp2525.simpledb.transaction.Transaction

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

type Slot = Int
type Flag = Int

class RecordPage private(tx: Transaction, val blockId: BlockId, layout: Layout):
  def getInt(slot: Slot, fieldName: FieldName): Try[Int] =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.getInt(blockId, fieldPosition)

  def getString(slot: Slot, fieldName: FieldName): Try[String] =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.getString(blockId, fieldPosition)

  def setInt(slot: Slot, fieldName: FieldName, value: Int): Try[Unit] =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.setInt(blockId, fieldPosition, value, true)

  def setString(slot: Slot, fieldName: FieldName, value: String): Try[Unit] =
    val fieldPosition = offset(slot) + layout.offset(fieldName)
    tx.setString(blockId, fieldPosition, value, true)

  def delete(slot: Slot): Try[Unit] =
    setFlag(slot, EMPTY)

  def format(): Try[Unit] =
    @tailrec
    def doFormat(slot: Slot): Try[Unit] =
      if isValidSlot(slot) then
        val result =
          tx.setInt(blockId, offset(slot), EMPTY, false).flatMap { _ =>
            Try {
              layout.schema.fields.foreach { fieldName =>
                val fieldPosition = offset(slot) + layout.offset(fieldName)
                layout.schema.`type`(fieldName) match
                  case FieldType.Integer =>
                    tx.setInt(blockId, fieldPosition, 0, false).get
                  case FieldType.Varchar =>
                    tx.setString(blockId, fieldPosition, "", false).get
              }
            }
          }
        result match
          case Success(()) => doFormat(slot + 1)
          case Failure(e) => Failure(e)
      else
        Success(())

    doFormat(0)

  def nextAfter(slot: Slot): Try[Slot] =
    searchAfter(slot, USED)

  def insertAfter(slot: Slot): Try[Slot] =
    searchAfter(slot, EMPTY) match
      case Success(newSlot) =>
        if newSlot >= 0 then
          setFlag(newSlot, USED).map(_ => newSlot)
        else
          Success(newSlot)
      case Failure(e) => Failure(e)

  private def setFlag(slot: Slot, flag: Flag): Try[Unit] =
    tx.setInt(blockId, offset(slot), flag, true)

  private def searchAfter(slot: Slot, flag: Flag): Try[Slot] =
    @tailrec
    def doSearchAfter(slot: Slot): Try[Slot] =
      val nextSlot: Slot = slot + 1
      if isValidSlot(nextSlot) then
        tx.getInt(blockId, offset(nextSlot)) match
          case Success(aFlag) =>
            if aFlag == flag then
              Success(nextSlot)
            else
              doSearchAfter(nextSlot)
          case Failure(e) => Failure(e)
      else
        Success(-1)

    doSearchAfter(slot)

  private def isValidSlot(slot: Slot): Boolean =
    offset(slot + 1) <= tx.blockSize

  private def offset(slot: Slot): Offset = slot * layout.slotSize

object RecordPage:
  val EMPTY: Int = 0
  val USED: Int = 1

  def apply(tx: Transaction, blockId: BlockId, layout: Layout): Try[RecordPage] =
    tx.pin(blockId).map(_ => new RecordPage(tx, blockId, layout))