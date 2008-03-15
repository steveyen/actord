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

// ------------------------------------------------

class MServerStorage(dir: File, numSubServers: Int) {
  if (!dir.isDirectory)
    throw new MStorageException("invalid directory: " + dir.getPath)
  if (numSubServers <= 0)
    throw new MStorageException("invalid numSubServers: " + numSubServers)

  // TODO: Need file rebalancing tools if numSubServers (or number of CPU's) changes.
  //  
  val subStorages = new Array[MSubServerStorage](numSubServers)
  for (i <- 0 until numSubServers) {
    val subDir = new File(dir.getPath + "/sub_" + i)
    
    // Each subStorage gets its own subdirectory.
    //
    subDir.mkdirs

    subStorages(i) = new MSubServerStorage(subDir)
  }
}

// ----------------------------------------------------

class MSubServerStorage(subDir: File) {
  // TODO: File rotation.
  //
  val f = new File(subDir + "/db_00000000.data")
  
  def HEADER_LENGTH = 300
  def HEADER_LINE   = "# actord data file, format: binary-0.0.1\n\n"
  def HEADER_SUFFIX = (0 until (HEADER_LENGTH - HEADER_LINE.length)).map(x => "\n").mkString
  def HEADER        = HEADER_LINE + HEADER_SUFFIX

  def ROOT_DEFAULT = "a#Fq9a2b3Kh5sYf8x001".getBytes // Sort of like a checkpoint marker.
  def ROOT_LENGTH  = ROOT_DEFAULT.length
  
  val ROOT_MARKER: Array[Byte] = {
    if (f.exists &&
        f.length >= (HEADER_LENGTH + ROOT_LENGTH).toLong) {
      // Read ROOT_MARKER from header area of existing file.
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
        o.writeInt(ROOT_LENGTH) // Note: same as appender.appendArray format, of array length.
        o.write(ROOT_DEFAULT)   // Note: same as appender.appendArray format, of array body.
        o.flush
      } finally {
        o.close
      }
      ROOT_DEFAULT
    }
  }
  
  val initialRootLoc: StorageLoc = {
    // Scan backwards for the last ROOT_MARKER.  Also, truncate file if found.
    //
    // TODO: Handle multi-file truncation during backwards scan.
    //
    val raf = new RandomAccessFile(f, "rws")
    try {
      val sizeOfInt  = 4
      val minimumPos = (HEADER_LENGTH + sizeOfInt).toLong

      val mArr = new Array[Byte](ROOT_LENGTH)
      var mPos = -1L
      var cPos = raf.length - ROOT_LENGTH.toLong
      while (mPos < 0L &&
             cPos >= minimumPos) {
        raf.seek(cPos)
        raf.read(mArr)
        if (mArr.deepEquals(ROOT_MARKER))
          mPos = cPos
        else
          cPos = cPos - 1L // TODO: Do a faster backwards scan.
      }

      if (mPos < minimumPos)
        throw new MStorageException("could not find ROOT_MARKER in file: " + f.getPath)
        
      // Truncate the file, because everything after the last ROOT_MARKER
      // is a data write/append that got only partially written,
      // perhaps due to a crash or process termination.
      //
      raf.setLength(mPos + ROOT_MARKER.length.toLong)

      // Negative loc values means it's a clean, just-initialized file.
      //
      if (mPos == minimumPos)
        StorageLoc(-1, -1L)
      else
        StorageLoc(0, mPos - sizeOfInt.toLong)
    } finally {
      raf.close
    }
  }
    
  val s = new SingleFileStorage(f)
  
  def storage: Storage = s
}

// ------------------------------------------------

class MPersistentSubServer(override val id: Int, 
                           override val limitMemory: Long, 
                           checkInterval: Int,             // In millisecs, to check for dirty data.
                           val ss: MSubServerStorage)
  extends MSubServer(id, limitMemory) {
  override def createSortedMap: immutable.SortedMap[String, MEntry] =
    startPersistence({
      val io = ss.storage
      val t  = new MEntryTreapStorable(TreapEmptyNode[String, MEntry], io)
      
      // If the storage has a treap root, load it.
      //
      // TODO: What about file versioning?
      //
      val locSize = io.storageLocSize
      val locRoot = ss.initialRootLoc
      if (locRoot.id >= 0 &&
          locRoot.position > locSize) {
        val loc = io.readAt(StorageLoc(locRoot.id, locRoot.position - locSize), _.readLoc)
        
        new MEntryTreapStorable(t.loadNodeAt(loc, None), io)
      } else
        t
    })
  
  def startPersistence(initialData: immutable.SortedMap[String, MEntry]): 
                                    immutable.SortedMap[String, MEntry] = {
    initialData match {
      case initialTreap: MEntryTreapStorable =>
        val asyncPersister = new Thread { 
          override def run { 
            var prevTreap = initialTreap
            while (true) {
              val beg = System.currentTimeMillis
              data match {
                case currTreap: MEntryTreapStorable => 
                  if (currTreap != prevTreap) {
                    val locRoot = currTreap.appendNode(currTreap.root)
                    
                    currTreap.io.append((loc, appender) => {
                      appender.appendLoc(locRoot)
                      appender.appendArray(ss.ROOT_MARKER, 0, ss.ROOT_MARKER.length)
                    })
                  }
                  prevTreap = currTreap
              }
              val end = System.currentTimeMillis
              val amt = checkInterval - (end - beg)
              if (amt > 0)
                Thread.sleep(amt)
            }
          } 
        }
        asyncPersister.start
    }

    initialData // Returns initialData for chainability.
  }
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
    val key     = new String(reader.readArray)
    val flags   = reader.readLong
    val expTime = reader.readLong
    val data    = reader.readArray
    val cid     = reader.readLong
    MEntry(key, flags, expTime, data.size, data, cid)
  }
  
  def rootStorable = root.asInstanceOf[TreapStorableNode[String, MEntry]]
}

