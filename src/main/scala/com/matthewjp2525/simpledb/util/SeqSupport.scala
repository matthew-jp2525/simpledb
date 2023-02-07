package com.matthewjp2525.simpledb.util

import scala.util.{Failure, Success, Try}

object SeqSupport:
  def foreachTry[A](list: Seq[A])(f: A => Try[Unit]): Try[Unit] =
    @annotation.tailrec
    def loop(remaining: Seq[A]): Try[Unit] =
      remaining match {
        case head +: tail => f(head) match {
          case Success(()) => loop(tail)
          case Failure(e) => Failure(e)
        }
        case _ => Success(())
      }
    loop(list)
