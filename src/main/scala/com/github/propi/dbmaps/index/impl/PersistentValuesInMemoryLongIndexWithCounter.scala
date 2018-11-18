package com.github.propi.dbmaps.index.impl

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import com.github.propi.dbmaps.Closeable
import com.github.propi.dbmaps.index.{Collectible, Removable, ValueIndex}
import com.github.propi.dbmaps.utils.FileOps._
import com.github.propi.dbmaps.utils.MutableNumber
import com.github.propi.dbmaps.utils.serialization.{Deserializer, SerializationSize, Serializer}

import scala.collection.concurrent.TrieMap

/**
  * Created by Vaclav Zeman on 17. 11. 2018.
  */
class PersistentValuesInMemoryLongIndexWithCounter[T] private(file: File)
                                                             (implicit serializer: Serializer[T], deserializer: Deserializer[T], serializationSize: SerializationSize[T])
 extends ValueIndex[Long, T] with Removable[Long, T] with Collectible[Long, T] with Closeable  {

  type T1 = (Long, T)

  private var counter = 0
  private var pointer: Long = 0
  private val addressMap = new TrieMap[Int, Map[Long, MutableNumber[Int]]]()

  private val channel = {
    FileChannel.open(getFileOrCreate(file, true).toPath, StandardOpenOption.READ)
  }

  def getIndex(x: T): Option[Long] = {
    addressMap.get(x.hashCode()).flatMap(_.iterator.flatMap(x => getValue(x._1).map(x._1 -> _)).find(y => y._2 == x)).map(_._1)
  }

  def getValue(x: Long): Option[T] = {
    val pos = (x * serializationSize.size) - serializationSize.size
    val buffer = ByteBuffer.allocate(serializationSize.size)
    val bytes = channel.read(buffer, pos)
    if (bytes != serializationSize.size) {
      None
    } else {
      Some(deserializer.deserialize(buffer.array()))
    }
  }

  private def saveItem(x: T): Unit = {
    val fos = new FileOutputStream(file, true)
    try {
      fos.write(Serializer.serialize(x))
    } finally {
      fos.close()
    }
  }

  def addValue(x: T): Long = addressMap.synchronized {
    val hashcode = x.hashCode()
    getIndex(x) match {
      case Some(x) =>
        addressMap(hashcode)(x) += 1
        x
      case None =>
        pointer += 1
        saveItem(x)
        addressMap += (hashcode -> (addressMap.getOrElse(hashcode, Map.empty) + (pointer -> new MutableNumber(1))))
        counter += 1
        pointer
    }
  }

  def removeValue(x: T): Unit = addressMap.synchronized {
    val hashcode = x.hashCode()
    for ((map, index) <- addressMap.get(hashcode).zip(getIndex(x))) {
      val x = map(index) -= 1
      if (x.value <= 0) {
        addressMap += (hashcode -> (map - index))
        counter -= 1
      }
      if (addressMap(hashcode).isEmpty) {
        addressMap -= hashcode
      }
    }
  }

  def removeIndex(x: Long): Unit = getValue(x).foreach(removeValue)

  def collection: Traversable[T1] = new Traversable[T1] {
    def foreach[U](f: T1 => U): Unit = addressMap.valuesIterator.flatten.flatMap(x => getValue(x._1).map(x._1 -> _)).foreach(f)
  }

  def size: Int = counter

  def close(): Unit = channel.close()
}

object PersistentValuesInMemoryLongIndexWithCounter {

  def apply[T](file: File)
              (implicit serializer: Serializer[T],
               deserializer: Deserializer[T],
               serializationSize: SerializationSize[T]): PersistentValuesInMemoryLongIndexWithCounter[T] = {
    new PersistentValuesInMemoryLongIndexWithCounter(file)
  }

}