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
package ff.actord

import java.io._

import scala.collection._
import scala.actors._
import scala.actors.Actor._

import ff.actord.Util._
import ff.collection._

class MStorageException(m: String) extends Exception(m)

class MServerStorage(dir: File, numSubServers: Int) {
  if (!dir.isDirectory)
    throw new MStorageException("invalid directory: " + dir.getPath)
  if (numSubServers <= 0)
    throw new MStorageException("invalid numSubServers: " + numSubServers)
  
  private val subStorages = new Array[MSubServerStorage](numSubServers)
  for (i <- 0 until numSubServers) {
    val subDir = new File(dir.getPath + "/sub_" + i)
    
    // Each subStorage gets its own subdirectory.
    //
    subDir.mkdirs

    subStorages(i) = new MSubServerStorage(subDir)
  }
  
  def subStorage(id: Int): Storage = subStorages(id).storage
}

// ----------------------------------------------------

class MSubServerStorage(subDir: File) {
  val f = new File(subDir + "/00000001.data")
  
  def storage: Storage = s
  
  val HEADER_LENGTH = 300
  val HEADER_LINE   = "# actord data file, format: binary-0.0.1\n\n"
  val HEADER_SUFFIX = (0 until (HEADER_LENGTH - HEADER_LINE.length)).map(x => "\n").mkString
  val HEADER        = HEADER_LINE + HEADER_SUFFIX

  val ROOT_DEFAULT = "9gUd9a2b3Kh5sYf8x001".getBytes
  val ROOT_LENGTH  = ROOT_DEFAULT.length
  val ROOT_MARKER: Array[Byte] = {
    if (f.exists &&
        f.length >= (HEADER_LENGTH + ROOT_LENGTH)) {
      // Read ROOT_MARKER from existing file.
      //
      if (!f.isFile)
        throw new MStorageException("not a file: " + f.getPath)
      if (!f.canRead)
        throw new MStorageException("could not read file: " + f.getPath)

      val i = new DataInputStream(new FileInputStream(f))
      try {
        i.skipBytes(HEADER_LENGTH)
        val n = i.readInt
        if (n != ROOT_LENGTH)
          throw new MStorageException("root length mismatch: " + n + " in file: " + f.getPath)
        val m = new Array[Byte](n)
        i.read(m)
        m
      } finally {
        i.close
      }
    } else {
      // Create and emit header for a brand new file.
      //
      f.delete
      val o = new DataOutputStream(new FileOutputStream(f))
      try {
        o.write(HEADER.getBytes)
        o.writeInt(ROOT_LENGTH)
        o.write(ROOT_DEFAULT)
        o.flush
      } finally {
        o.close
      }
      ROOT_DEFAULT
    }
  }
  
  val initialRootLoc = 0
    
  val s = new SingleFileStorage(f)
}

// ------------------------------------------------

class MEntryTreapStorable(override val root: TreapNode[String, MEntry],
                  override val io: Storage)
  extends TreapStorable[String, MEntry](root, io) {
  override def mkTreap(r: TreapNode[String, MEntry]): Treap[String, MEntry] = 
    new MEntryTreapStorable(r, io)    
  
  def serializeKey(x: String): Array[Byte]     = x.getBytes
  def unserializeKey(arr: Array[Byte]): String = new String(arr)

  def serializeValue(x: MEntry, loc: StorageLoc, appender: StorageLocAppender): Unit = {
    val arr = x.key.getBytes
    appender.appendArray(arr, 0, arr.length)
    appender.appendLong(x.flags)
    appender.appendLong(x.expTime)
    appender.appendArray(x.data, 0, x.data.length)
    appender.appendLong(x.cid)
  }
    
  def unserializeValue(loc: StorageLoc, reader: StorageLocReader): MEntry = {
    val key = new String(reader.readArray)
    val flags = reader.readLong
    val expTime = reader.readLong
    val data = reader.readArray
    val cid = reader.readLong
    MEntry(key, flags, expTime, data.size, data, cid)
  }

  def rootStorable = root.asInstanceOf[TreapStorableNode[String, MEntry]]
}

