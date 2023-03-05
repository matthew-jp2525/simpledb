package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.{FieldName, RID}

import scala.annotation.tailrec

class SelectScan(scan: Scan,
                 predicate: Predicate) extends UpdateScan:
  def setInt(fieldName: FieldName, value: Int): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.setInt(fieldName, value)

  def setString(fieldName: FieldName, value: String): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.setString(fieldName, value)

  def setVal(fieldName: FieldName, value: Constant): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.setVal(fieldName, value)

  def insert(): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.insert()

  def delete(): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.delete()

  def getRid: RID =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.getRid

  def moveToRid(rid: RID): Unit =
    val updateScan = scan.asInstanceOf[UpdateScan]
    updateScan.moveToRid(rid)

  def beforeFirst(): Unit = scan.beforeFirst()

  def next(): Boolean =
    @tailrec
    def go: Boolean =
      if scan.next() then
        if predicate.isSatisfied(scan) then
          true
        else
          go
      else
        false

    go

  def getInt(fieldName: FieldName): Int = scan.getInt(fieldName)

  def getString(fieldName: FieldName): String = scan.getString(fieldName)

  def getVal(fieldName: FieldName): Constant = scan.getVal(fieldName)

  def hasField(fieldName: FieldName): Boolean = scan.hasField(fieldName)

  def close(): Unit = scan.close()
