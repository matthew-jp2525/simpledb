package com.matthewjp2525.simpledb.query

import com.matthewjp2525.simpledb.query.Expression.{ConstantExpression, FieldNameExpression}
import com.matthewjp2525.simpledb.record.SchemaOps.*
import com.matthewjp2525.simpledb.record.{FieldName, Schema}

enum Expression:
  case ConstantExpression(constant: Constant)
  case FieldNameExpression(fieldName: FieldName)

  def isFieldName: Boolean = this match
    case FieldNameExpression(_fieldName) => true
    case ConstantExpression(_constant) => false

  def asConstant: Constant = this match
    case FieldNameExpression(_fieldName) =>
      throw new RuntimeException("cannot get constant.")
    case ConstantExpression(constant) => constant

  def asFieldName: FieldName = this match
    case FieldNameExpression(fieldName) => fieldName
    case ConstantExpression(_constant) =>
      throw new RuntimeException("cannot get field name.")

  def evaluate(scan: Scan): Constant = this match
    case FieldNameExpression(fieldName) => scan.getVal(fieldName)
    case ConstantExpression(constant) => constant

  def appliesTo(schema: Schema): Boolean = this match
    case FieldNameExpression(fieldName) => schema.hasField(fieldName)
    case ConstantExpression(_constant) => true

  override def toString: String = this match
    case FieldNameExpression(fieldName) => fieldName
    case ConstantExpression(constant) => constant.toString

object Expression:
  def apply(constant: Constant): Expression = ConstantExpression(constant)

  def apply(fieldName: FieldName): Expression = FieldNameExpression(fieldName)
