package com.github.propi.dbmaps.kv.impl

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util

import com.github.propi.dbmaps.Closeable
import com.github.propi.dbmaps.kv.{KeyValueMap, ValueResolver, ValueStorage}
import com.github.propi.dbmaps.utils.serialization.{Deserializer, SerializationSize, Serializer}

/**
  * Created by Vaclav Zeman on 18. 11. 2018.
  */
class LinearHashingKeyValueMap[K, V, VR <: ValueResolver[V]] private(dir: File,
                                                                     numOfBuckets: Int,
                                                                     bucketSize: Int,
                                                                     overflowBucketSize: Int)
                                                                    (implicit serializer: Serializer[K],
                                                                     deserializer: Deserializer[K],
                                                                     serializationSize: SerializationSize[K],
                                                                     valueStorage: ValueStorage[V, VR])
  extends KeyValueMap[K, V, VR] with Closeable {

  if (!dir.isDirectory) dir.mkdirs()

  private val keyValueBytes = 1 + serializationSize.size + 8
  private val keyValueCollectionBytes = keyValueBytes * bucketSize
  private val bucketBytes = keyValueCollectionBytes + 8
  private val overflowKeyValueCollectionBytes = keyValueBytes * overflowBucketSize
  private val overflowBucketBytes = overflowKeyValueCollectionBytes + 8

  private lazy val hmapRaf = new RandomAccessFile(new File(dir, "h.map"), "rw")
  private lazy val overflowRaf = new RandomAccessFile(new File(dir, "ovf.map"), "rw")

  private lazy val hmapChannel = hmapRaf.getChannel
  private lazy val overflowChannel = overflowRaf.getChannel

  private def getKeyValue(storage: FileChannel)(pointer: Long): Option[(Array[Byte], Long)] = {
    val buffer = ByteBuffer.allocate(keyValueBytes)
    val redBytes = storage.read(buffer, pointer)
    if (keyValueBytes == redBytes) {
      if (buffer.get() == 1) {
        val key = new Array[Byte](serializationSize.size)
        val value = buffer.get(key).getLong
        Some(key -> value)
      } else {
        None
      }
    } else {
      None
    }
  }

  private def findValueByKey(storage: FileChannel, startPointer: Long, keyValueCollectionBytes: Int, key: Array[Byte]): Either[Long, Option[Long]] = {
    val keyValueIt = (startPointer until (startPointer + keyValueCollectionBytes) by keyValueBytes).iterator.map(getKeyValue(storage)).zipWithIndex
    var emptySlot = Option.empty[Long]
    for ((kv, index) <- keyValueIt) kv match {
      case Some((k, v)) if util.Arrays.equals(key, k) => return Left(v)
      case None if emptySlot.isEmpty => emptySlot = Some(startPointer + (keyValueBytes * index))
      case _ =>
    }
    Right(emptySlot)
  }

  private def findInHmap(hkey: Int, key: Array[Byte]): Either[Long, Option[Long]] = {
    val startPointer = hkey.toLong * bucketBytes
    findValueByKey(hmapChannel, startPointer, keyValueCollectionBytes, key)
  }

  @scala.annotation.tailrec
  private def findInOverflow(pointer: Long, key: Array[Byte], emptySlot: Option[Long] = None): Either[Long, Option[Long]] = {
    findValueByKey(overflowChannel, pointer, overflowKeyValueCollectionBytes, key) match {
      case v@Left(_) => v
      case Right(emptySlot2) =>
        val buffer = ByteBuffer.allocate(8)
        val redBytes = overflowChannel.read(buffer, pointer + overflowKeyValueCollectionBytes)
        if (redBytes == buffer.capacity()) {
          val nextPointer = buffer.getLong
          if (nextPointer == 0) {
            Right(emptySlot.orElse(emptySlot2))
          } else {
            findInOverflow(nextPointer, key, emptySlot.orElse(emptySlot2))
          }
        } else {
          Right(emptySlot.orElse(emptySlot2))
        }
    }
  }

  def add(kv: (K, V)): Boolean = ???

  def get(k: K): Option[VR] = ???

  def close(): Unit = {
    hmapChannel.close()
    hmapRaf.close()
    overflowChannel.close()
    overflowRaf.close()
  }

}
