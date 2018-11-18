package com.github.propi.dbmaps.index

/**
  * Created by Vaclav Zeman on 17. 11. 2018.
  */
trait Collectible[A, B] {

  self: ValueIndex[A, B] =>

  def collection: Traversable[(A, B)]

  def size: Int

}
