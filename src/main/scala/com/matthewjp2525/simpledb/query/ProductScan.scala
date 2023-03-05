package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.FieldName

class ProductScan private(scan1: Scan, scan2: Scan) extends Scan:
  def beforeFirst(): Unit =
    scan1.beforeFirst()
    scan1.next()
    scan2.beforeFirst()

  def next(): Boolean =
    if scan2.next() then
      true
    else
      scan2.beforeFirst()
      scan2.next() && scan1.next()

  def getInt(fieldName: FieldName): Int =
    if scan1.hasField(fieldName) then
      scan1.getInt(fieldName)
    else
      scan1.getInt(fieldName)

  def getString(fieldName: FieldName): String =
    if scan1.hasField(fieldName) then
      scan1.getString(fieldName)
    else
      scan2.getString(fieldName)

  def getVal(fieldName: FieldName): Constant =
    if scan1.hasField(fieldName) then
      scan1.getVal(fieldName)
    else
      scan2.getVal(fieldName)

  def hasField(fieldName: FieldName): Boolean =
    scan1.hasField(fieldName) || scan2.hasField(fieldName)

  def close(): Unit =
    scan1.close()
    scan2.close()

object ProductScan:
  def apply(scan1: Scan, scan2: Scan): ProductScan =
    scan1.next()
    new ProductScan(scan1, scan2)