package com.github.propi.dbmaps.index

import com.github.propi.dbmaps.Closeable

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by Vaclav Zeman on 26. 9. 2018.
  */
trait ValueIndex[A, B] {

  def getIndex(x: B): Option[A]

  def getValue(x: A): Option[B]

  def addValue(x: B): A

}

object ValueIndex {

  def use[A, B, T <: ValueIndex[A, B] with Closeable, R](b: => T)(f: T => R): R = {
    val x = b
    try {
      f(x)
    } finally {
      x.close()
    }
  }

  def useF[A, B, T <: ValueIndex[A, B] with Closeable, R](b: => T)(f: T => Future[R])(implicit ec: ExecutionContext): Future[R] = Future {
    val x = b
    val r = f(x)
    r.onComplete(_ => x.close())
    r
  }.flatten

}