package com.github.propi.dbmaps.kv

/**
  * Created by Vaclav Zeman on 17. 11. 2018.
  */
trait KeyValueMap[K, V, VR <: ValueResolver[V]] {

  type KV = (K, V)

  def add(kv: KV): Unit

  final def add(key: K, value: V): Unit = add(key -> value)

  final def +(kv: KV): this.type = {
    add(kv)
    this
  }

  def get(k: K): Option[VR]

}
