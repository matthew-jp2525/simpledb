package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.{FieldName, RID}

trait UpdateScan extends Scan:
  def setInt(fieldName: FieldName, value: Int): Unit
  def setString(fieldName: FieldName, value: String): Unit
  def setVal(fieldName: FieldName, value: Constant): Unit
  def insert(): Unit
  def delete(): Unit
  def getRid: RID
  def moveToRid(rid: RID): Unit

