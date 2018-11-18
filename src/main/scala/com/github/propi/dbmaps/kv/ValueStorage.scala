package com.github.propi.dbmaps.kv

/**
  * Created by Vaclav Zeman on 18. 11. 2018.
  */
trait ValueStorage[V, VR <: ValueResolver[V]] {

  def add(value: V): Long

  def get(pointer: Long): VR

}
