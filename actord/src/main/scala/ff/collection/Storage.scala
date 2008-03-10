/**
 * Copyright 2008 Steve Yen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ff.collection

import scala.collection._

import java.io._

/** 
 * Interface for a journaling, append-only nonvolatile storage.
 */
trait Storage {
  def readAt[T](loc: StorageLoc, func: StorageReader => T): T
  def append(func: (StorageLoc, StorageAppender) => Unit): StorageLoc
}

trait StorageReader {
  def readArray: Array[Byte]
  def readLoc: StorageLoc
  def readUTF: String
}

trait StorageAppender {
  def appendArray(arr: Array[Byte], offset: Int, len: Int): StorageLoc
  def appendLoc(loc: StorageLoc): Unit
  def appendUTF(s: String): StorageLoc
}

/**
 * Immutable opaque pointer to location in storage.
 * The id is an opaque identifier to a storage shard, possibly 
 * representing a file.  The position is also opaque, possibly
 * representing a byte offset in a file.  
 */
case class StorageLoc(id: Short, position: Long)

object NullStorageLoc extends StorageLoc(-1, -1L)

// ---------------------------------------------------------

/**
 * A storage swizzle holds a pair of a storage location 
 * and an in-memory value.
 *
 * An unsaved swizzle has a in-memory value but no location yet.
 * An unloaded swizzle has a storage location, but has null 
 * in-memory value.
 */
class StorageSwizzle[S <: AnyRef] {
  private var loc_i: StorageLoc = null
  private var value_i: S        = _

  def loc: StorageLoc = synchronized { loc_i }
  def loc_!!(x: StorageLoc) = synchronized { 
    if (x != null && loc_i != null)
      throw new RuntimeException("cannot override an existing swizzle loc")
    loc_i = x
    loc_i 
  }

  def value: S = synchronized { value_i }
  def value_!!(x: S) = synchronized { 
    if (x != null && value_i != null)
      throw new RuntimeException("cannot overwrite an existing swizzle value")
    value_i = x
    value_i
  }
}

// ---------------------------------------------------------

class SingleFileStorage(f: File) extends Storage {
  val fos            = new FileOutputStream(f, true)
  val fosData        = new DataOutputStream(new BufferedOutputStream(fos))
  val fosChannel     = fos.getChannel
  val fosInitialSize = fosChannel.size
  
  val raf = new RandomAccessFile(f, "r")
  
  val reader = new StorageReader {
    def readArray: Array[Byte] = {
      val len = raf.readInt
      val arr = new Array[Byte](len)
      raf.readFully(arr)
      arr
    }
        
    def readLoc: StorageLoc = StorageLoc(raf.readShort, raf.readLong)
    def readUTF: String     = raf.readUTF
  }
  
  val appender = new StorageAppender {
    def appendArray(arr: Array[Byte], offset: Int, len: Int): StorageLoc = {
      val loc = StorageLoc(0, fosChannel.size)
      fosData.writeInt(len)
      fosData.write(arr, offset, len)
      loc
    }
    
    def appendLoc(loc: StorageLoc): Unit = {
      fosData.writeShort(loc.id)
      fosData.writeLong(loc.position)
    }
    
    def appendUTF(s: String): StorageLoc = {
      val loc = StorageLoc(0, fosChannel.size)
      fosData.writeUTF(s)
      loc
    }
  }

  def readAt[T](loc: StorageLoc, func: StorageReader => T): T = {
    if (loc == null)
      throw new RuntimeException("bad loc during SFS readArray: " + loc)
    if (loc.id != 0)
      throw new RuntimeException("bad loc id during SFS readArray: " + loc)
    if (loc.position >= raf.length)
      throw new RuntimeException("bad loc position during SFS readArray: " + loc)

    synchronized {
      raf.seek(loc.position)
      func(reader)
    }
  }

  def append(func: (StorageLoc, StorageAppender) => Unit): StorageLoc = 
    synchronized {
      val loc = StorageLoc(0, fosChannel.size)
      func(loc, appender)
      loc
    }
}
