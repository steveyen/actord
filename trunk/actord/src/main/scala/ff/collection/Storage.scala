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
 * The interface abstracts away underlying details of actual files,
 * whether single or multiple, whether auto-rotated, compacted, encrypted, etc.
 */
trait StorageReader {
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T
  def close: Unit
}

trait StorageAppender {
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc
  def close: Unit
}

trait Storage extends StorageReader with StorageAppender

// ---------------------------------------------------------

trait StorageLocReader {
  def readArray: Array[Byte]
  def readLoc: StorageLoc
  def readUTF: String
}

trait StorageLocAppender {
  def appendArray(arr: Array[Byte], offset: Int, len: Int): Unit
  def appendLoc(loc: StorageLoc): Unit
  def appendUTF(s: String): Unit
  def flush: Unit
}

// ---------------------------------------------------------

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
 * and an in-memory value.  Valid states are...
 * - an unsaved swizzle has a in-memory value but no location yet.
 * - an unloaded swizzle has a storage location, but has null 
 *   in-memory value.
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

/**
 * A simple storage implementation that accesses a single file.
 */
class SingleFileStorageReader(f: File) extends StorageReader {
  private val raf = new RandomAccessFile(f, "r")
  
  def close = synchronized { raf.close }
  
  private val reader = new StorageLocReader {
    def readArray: Array[Byte] = {
      val len = raf.readInt
      val arr = new Array[Byte](len)
      raf.readFully(arr)
      arr
    }
        
    def readLoc: StorageLoc = StorageLoc(raf.readShort, raf.readLong)
    def readUTF: String     = raf.readUTF
  }
  
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T = {
    checkLoc(loc)

    synchronized {
      raf.seek(loc.position)
      func(reader)
    }
  }
  
  def checkLoc(loc: StorageLoc) = {
    if (loc == null)
      throw new RuntimeException("bad loc during SFS readAt: " + loc)
    if (loc.id != 0)
      throw new RuntimeException("bad loc id during SFS readAt: " + loc)
    if (loc.position >= raf.length)
      throw new RuntimeException("bad loc position during SFS readAt: " + loc)
  }
}

// ---------------------------------------------------------

/**
 * A simple storage implementation that appends to a single file.
 */
class SingleFileStorage(f: File) extends SingleFileStorageReader(f) with Storage {
  private val fos        = new FileOutputStream(f, true)
  private val fosData    = new DataOutputStream(new BufferedOutputStream(fos))
  private val fosChannel = fos.getChannel
  
  override def close = synchronized {
    appender.flush
    fosData.close
    super.close
  }

  private val appender = new StorageLocAppender {
    def appendArray(arr: Array[Byte], offset: Int, len: Int): Unit = {
      fosData.writeInt(len)
      fosData.write(arr, offset, len)
    }
    
    def appendLoc(loc: StorageLoc): Unit = {
      fosData.writeShort(loc.id)
      fosData.writeLong(loc.position)
    }
    
    def appendUTF(s: String): Unit = 
      fosData.writeUTF(s)
    
    def flush = {
      fosData.flush
      fosChannel.force(true) // TODO: Do we really need both flush and force?
    }
  }

  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc = 
    synchronized {
      val loc = StorageLoc(0, fosChannel.size)
      func(loc, appender)
      appender.flush
      loc
    }
}
