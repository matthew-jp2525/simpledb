package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.FieldName

trait Scan extends AutoCloseable:
  def beforeFirst(): Unit
  def next(): Boolean
  def getInt(fieldName: FieldName): Int
  def getString(fieldName: FieldName): String
  def getVal(fieldName: FieldName): Constant
  def hasField(fieldName: FieldName): Boolean
  def close(): Unit

