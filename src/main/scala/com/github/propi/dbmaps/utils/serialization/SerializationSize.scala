package com.github.propi.dbmaps.utils.serialization

import NumericByteArray._

/**
  * Created by Vaclav Zeman on 1. 8. 2017.
  */
trait SerializationSize[T] {

  val size: Int

}

object SerializationSize {

  implicit def numberSerializationSize[T <: AnyVal](implicit n: Numeric[T]): SerializationSize[T] = apply(numberToByteArray(n.one).length)

  implicit val booleanSerializationSize: SerializationSize[Boolean] = apply(1)

  implicit def default[T <: AnyRef]: SerializationSize[T] = apply(-1)

  def apply[T](n: Int): SerializationSize[T] = new SerializationSize[T] {
    val size: Int = n
  }

}