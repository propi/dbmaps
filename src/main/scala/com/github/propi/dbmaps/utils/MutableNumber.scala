package com.github.propi.dbmaps.utils

/**
  * Created by Vaclav Zeman on 29. 11. 2017.
  */
class MutableNumber[T](private var i: T)(implicit n: Numeric[T]) {

  def this()(implicit n: Numeric[T]) = this(n.zero)

  def value: T = i

  def +=(x: T): this.type = {
    i = n.plus(i, x)
    this
  }

  def -=(x: T): this.type = {
    i = n.minus(i, x)
    this
  }

  def set(x: T): this.type = {
    i = x
    this
  }

}
