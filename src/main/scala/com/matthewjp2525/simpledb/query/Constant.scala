package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.query.Constant.{IntValue, StringValue}

enum Constant extends Comparable[Constant]:
  case IntValue(value: Int)
  case StringValue(value: String)

  override def compareTo(another: Constant): Int = this match
    case IntValue(value) => value.compareTo(another.asInt)
    case StringValue(value) => value.compareTo(another.asString)

  def asInt: Int = this match
    case IntValue(value) => value
    case StringValue(_value) =>
      throw new RuntimeException("cannot get int value.")

  def asString: String = this match
    case IntValue(_value) =>
      throw new RuntimeException("cannot get string value.")
    case StringValue(value) => value

  override def hashCode(): Int = this match
    case IntValue(value) => value.hashCode()
    case StringValue(value) => value.hashCode

  def equals(another: Constant): Boolean = this match
    case IntValue(value) => value.equals(another.asInt)
    case StringValue(value) => value.equals(another.asString)

  override def toString: String = this match
    case IntValue(value) => value.toString
    case StringValue(value) => value

object Constant:
  def apply(value: Int): IntValue = IntValue(value)

  def apply(value: String): StringValue = StringValue(value)