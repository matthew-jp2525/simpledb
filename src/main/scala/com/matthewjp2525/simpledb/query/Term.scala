package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.record.Schema

case class Term(
               lhs: Expression,
               rhs: Expression
               ):
  def isSatisfied(scan: Scan): Boolean =
    val lhsValue = lhs.evaluate(scan)
    val rhsValue = rhs.evaluate(scan)
    rhsValue.equals(lhsValue)

  def appliesTo(schema: Schema): Boolean =
    lhs.appliesTo(schema) && rhs.appliesTo(schema)

  override def toString: String = lhs.toString + "=" + rhs.toString
