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

trait Storage {
  def readArray(loc: StorageLoc): Array[Byte]
  def appendArray(arr: Array[Byte], offset: Int, len: Int): StorageLoc
  def flush: Unit
}

// ---------------------------------------------------------

case class StorageLoc(id: Int, position: Long)

// ---------------------------------------------------------

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
  
  def readArray(loc: StorageLoc): Array[Byte] = {
    if (loc == null)
      throw new RuntimeException("bad loc during SFS readArray: " + loc)
    if (loc.id != 0)
      throw new RuntimeException("bad loc id during SFS readArray: " + loc)
    if (loc.position >= raf.length)
      throw new RuntimeException("bad loc position during SFS readArray: " + loc)

    synchronized {
      raf.seek(loc.position)
      val len = raf.readInt
      val arr = new Array[Byte](len)
      raf.readFully(arr)
      arr
    }
  }
  
  def appendArray(arr: Array[Byte], offset: Int, len: Int): StorageLoc = {
    var pos = -1L
    synchronized {
      pos = fosChannel.size
      fosData.writeInt(len)
      fosData.write(arr, offset, len)
    }
    StorageLoc(0, pos)
  }
  
  def flush = synchronized {
    fosData.flush
    fosChannel.force(true)
  }
}
