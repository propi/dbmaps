package com.github.propi.dbmaps.kv

/**
  * Created by Vaclav Zeman on 17. 11. 2018.
  */
trait Removable[K] {

  self: KeyValueMap[K, _, _] =>

  def remove(k: K): Boolean

  final def -(k: K): this.type = {
    remove(k)
    this
  }

}
