package com.matthewjp2525.simpledb.query

import scala.annotation.tailrec

case class Predicate private(terms: List[Term]):
  def conjoinWith(predicate: Predicate): Predicate =
    this.copy(terms = terms ++ predicate.terms)

  def isSatisfied(scan: Scan): Boolean =
    @tailrec
    def go(terms: List[Term]): Boolean = terms match
      case Nil => true
      case head :: tail =>
        if !head.isSatisfied(scan) then
          false
        else
          go(tail)

    go(terms)

  override def toString: String =
    terms.foldLeft("") { (acc, term) =>
      acc + " and " + term.toString
    }

object Predicate:
  def apply(term: Term): Predicate = new Predicate(term :: Nil)

  def apply(): Predicate = new Predicate(Nil)