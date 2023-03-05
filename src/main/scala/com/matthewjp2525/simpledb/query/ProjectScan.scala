package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.FieldName

class ProjectScan (scan: Scan, fieldList: List[FieldName]) extends Scan:
  def beforeFirst(): Unit = scan.beforeFirst()

  def next(): Boolean = scan.next()

  def getInt(fieldName: FieldName): Int =
    if hasField(fieldName) then
      scan.getInt(fieldName)
    else
      throw new RuntimeException(s"field (${fieldName}) not found.")

  def getString(fieldName: FieldName): String =
    if hasField(fieldName) then
      scan.getString(fieldName)
    else
      throw new RuntimeException(s"field (${fieldName}) not found.")

  def getVal(fieldName: FieldName): Constant =
    if hasField(fieldName) then
      scan.getVal(fieldName)
    else
      throw new RuntimeException(s"field (${fieldName}) not found.")

  def hasField(fieldName: FieldName): Boolean =
    fieldList.contains(fieldName)

  def close(): Unit = scan.close()
