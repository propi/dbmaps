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
                                                                     initNumOfBuckets: Int,
                                                                     bucketSize: Int,
                                                                     overflowBucketSize: Int,
                                                                     adjustThreshold: Double,
                                                                     adjustNumber: Int)
                                                                    (implicit serializer: Serializer[K],
                                                                     deserializer: Deserializer[K],
                                                                     serializationSize: SerializationSize[K],
                                                                     valueStorage: ValueStorage[V, VR])
  extends KeyValueMap[K, V, VR] with Closeable {

  if (!dir.isDirectory) dir.mkdirs()

  private val keyValueBytes = 1 + serializationSize.size + 8

  private var numOfBuckets = initNumOfBuckets
  private var numOfKeys = 0

  private sealed trait KeyState {
    val bucket: Bucket
  }

  private object KeyState {

    sealed trait Empty extends KeyState {
      val pointer: Long

      def save(key: Array[Byte], value: V): Filled = {
        val buffer = ByteBuffer.allocate(keyValueBytes)
        val valuePointer = valueStorage.add(value)
        buffer.put(1.toByte).put(key).putLong(valuePointer)
        bucket.storage.channel.write(buffer, pointer)
        numOfKeys += 1
        Filled(key, valuePointer)(bucket)
      }
    }

    case class Deleted(pointer: Long)(val bucket: Bucket) extends Empty

    case class NotInit(pointer: Long)(val bucket: Bucket) extends Empty

    case class Filled(key: Array[Byte], value: Long)(val bucket: Bucket) extends KeyState

    def apply(bucket: Bucket)(pointer: Long): KeyState = {
      val buffer = ByteBuffer.allocate(keyValueBytes)
      val redBytes = bucket.storage.channel.read(buffer, pointer)
      if (keyValueBytes == redBytes) {
        buffer.get() match {
          case 1 =>
            val key = new Array[Byte](serializationSize.size)
            val value = buffer.get(key).getLong
            Filled(key, value)(bucket)
          case 2 => Deleted(pointer)(bucket)
          case _ => NotInit(pointer)(bucket)
        }
      } else {
        NotInit(pointer)(bucket)
      }
    }

  }

  private abstract class Storage(file: File, val bucketSize: Int) {
    val keyValueCollectionBytes: Int = keyValueBytes * bucketSize
    val bucketBytes: Int = keyValueCollectionBytes + 8

    private lazy val raf = new RandomAccessFile(file, "rw")
    lazy val channel: FileChannel = raf.getChannel

    def getBucket(pointer: Long): Bucket = new Bucket(pointer)(this)

    def getBucket(num: Int): Bucket = getBucket(num.toLong * bucketBytes)

    @scala.annotation.tailrec
    final def findKeyRecursively(key: Array[Byte], bucket: Bucket, emptySlot: Option[KeyState.Empty] = None): Either[KeyState, Bucket] = {
      bucket.findKey(key) match {
        case v@Left(_: KeyState.Filled) => v
        case Left(x: KeyState.Empty) => bucket.nextBucket match {
          case Some(nextBucket) => findKeyRecursively(key, nextBucket, emptySlot.orElse(Some(x)))
          case None => Left(emptySlot.getOrElse(x))
        }
        case Right(_) => bucket.nextBucket match {
          case Some(nextBucket) => findKeyRecursively(key, nextBucket, emptySlot)
          case None => emptySlot.map(Left(_)).getOrElse(Right(bucket))
        }
      }
    }

    def close(): Unit = {
      channel.close()
      raf.close()
    }
  }

  private object OverflowStorage extends Storage(new File(dir, "ovf.map"), overflowBucketSize) {
    private var overflowNumOfBuckets: Int = 0

    def createBucket: Bucket = {
      val bucket = getBucket(overflowNumOfBuckets)
      overflowNumOfBuckets += 1
      bucket
    }
  }

  private object HmapStorage extends Storage(new File(dir, "h.map"), bucketSize) {

  }

  private class Bucket private(val keyStates: Traversable[KeyState], val pointer: Long, _nextPointer: => Option[Long])(implicit val storage: Storage) {
    def this(pointer: Long)(implicit storage: Storage) = this(
      (pointer until (pointer + storage.keyValueCollectionBytes) by keyValueBytes).view.map(KeyState(this)),
      pointer,
      getNextPointer(pointer + storage.keyValueCollectionBytes)
    )

    lazy val nextPointer: Option[Long] = _nextPointer

    def nextBucket: Option[Bucket] = nextPointer.map(new Bucket(_))

    def addNextBucket(newBucket: Bucket): KeyState.NotInit = {
      val buffer = ByteBuffer.allocate(8)
      buffer.putLong(newBucket.pointer + 1)
      storage.channel.write(buffer, pointer + storage.keyValueCollectionBytes)
      KeyState.NotInit(newBucket.pointer)(newBucket)
    }

    def findKey(key: Array[Byte]): Either[KeyState, Bucket] = {
      var emptySlot = Option.empty[KeyState]
      for (keyState <- keyStates) {
        keyState match {
          case x@KeyState.Filled(k, _) if util.Arrays.equals(key, k) => return Left(x)
          case x: KeyState.NotInit => return Left(x)
          case x: KeyState.Deleted if emptySlot.isEmpty => emptySlot = Some(x)
          case _ =>
        }
      }
      emptySlot.map(Left(_)).getOrElse(Right(this))
    }

  }

  object Bucket {


  }

  private def getNextPointer(pointer: Long)(implicit storage: Storage): Option[Long] = {
    val buffer = ByteBuffer.allocate(8)
    val redBytes = storage.channel.read(buffer, pointer)
    if (redBytes == buffer.capacity()) {
      val nextPointer = buffer.getLong
      if (nextPointer == 0) {
        None
      } else {
        Some(nextPointer - 1)
      }
    } else {
      None
    }
  }

  private def numOfBits: Int = math.ceil(math.log(numOfBuckets) / math.log(2)).toInt

  private def getAverageOccupancy: Double = numOfKeys / (numOfBuckets.toLong * bucketSize)

  private def shouldAdjust: Boolean = getAverageOccupancy > adjustThreshold

  /*private def getKeyValue(storage: FileChannel)(pointer: Long): KeyState = {
    val buffer = ByteBuffer.allocate(keyValueBytes)
    val redBytes = storage.read(buffer, pointer)
    if (keyValueBytes == redBytes) {
      KeyState(buffer)
    } else {
      KeyState.Empty
    }
  }

  private def findValueByKey(storage: FileChannel, startPointer: Long, keyValueCollectionBytes: Int, key: Array[Byte]): Either[Long, Option[Long]] = {
    val keyValueIt = (startPointer until (startPointer + keyValueCollectionBytes) by keyValueBytes).iterator.map(getKeyValue(storage)).zipWithIndex
    var emptySlot = Option.empty[Long]
    for ((kv, index) <- keyValueIt) kv match {
      case KeyState.Filled(k, v) if util.Arrays.equals(key, k) => return Left(v)
      case KeyState.Empty => return Right(Some(startPointer + (keyValueBytes * index)))
      case KeyState.Deleted if emptySlot.isEmpty => emptySlot = Some(startPointer + (keyValueBytes * index))
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

  private def saveKey(storage: FileChannel, pointer: Long, key: Array[Byte], valuePointer: Long): Boolean = {
    val buffer = ByteBuffer.allocate(keyValueBytes)
    buffer.put(1.toByte).put(key).putLong(valuePointer)
    storage.write(buffer, pointer)
    true
  }

  private def getOverflowPointer(hkey: Int): Option[Long] = {
    val startPointer = hkey.toLong * bucketBytes + keyValueCollectionBytes
    val buffer = ByteBuffer.allocate(8)
    val redBytes = hmapChannel.read(buffer, startPointer)
    if (redBytes == buffer.capacity()) {
      val overflowPointer = buffer.getLong
      if (overflowPointer == 0) None else Some(overflowPointer)
    } else {
      None
    }
  }*/

  private def getHashedKey(k: K): Int = {
    val bs = Integer.toBinaryString(k.hashCode()).takeRight(numOfBits)
    val x = Integer.parseInt(bs, 2)
    if (x > numOfBuckets - 1) {
      Integer.parseInt(bs.drop(1), 2)
    } else {
      x
    }
  }

  def add(kv: (K, V)): Unit = {
    val key = serializer.serialize(kv._1)
    val hkey = getHashedKey(kv._1)
    val hmapBucket = HmapStorage.getBucket(hkey)
    hmapBucket.findKey(key) match {
      case Left(value) => value match {
        case x: KeyState.Filled => valueStorage.get(x.value).add(kv._2)
        case x: KeyState.Empty => x.bucket.nextPointer match {
          case Some(overflowPointer) => OverflowStorage.findKeyRecursively(key, OverflowStorage.getBucket(overflowPointer)) match {
            case Left(KeyState.Filled(_, v)) => valueStorage.get(v).add(kv._2)
            case _ => x.save(key, kv._2)
          }
          case None => x.save(key, kv._2)
        }
      }
      case Right(fullBucket) => fullBucket.nextPointer
        .map(op => OverflowStorage.findKeyRecursively(key, OverflowStorage.getBucket(op)))
        .getOrElse(Right(fullBucket)) match {
        case Left(KeyState.Filled(_, v)) => valueStorage.get(v).add(kv._2)
        case Left(x: KeyState.Empty) => x.save(key, kv._2)
        case Right(bucket) => bucket.addNextBucket(OverflowStorage.createBucket).save(key, kv._2)
      }
    }
  }

  def get(k: K): Option[VR] = ???

  def close(): Unit = {
    OverflowStorage.close()
    HmapStorage.close()
  }

}
