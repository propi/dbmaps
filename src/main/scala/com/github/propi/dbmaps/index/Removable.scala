package com.github.propi.dbmaps.index

/**
  * Created by Vaclav Zeman on 17. 11. 2018.
  */
trait Removable[A, B] {

  self: ValueIndex[A, B] =>

  def removeValue(x: B): Unit

  def removeIndex(x: A): Unit

}
