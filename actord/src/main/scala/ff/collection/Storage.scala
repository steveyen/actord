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

class StorageException(m: String) extends Exception(m)

/** 
 * Interface for a journaling, append-only nonvolatile storage.
 * The interface abstracts away underlying details of actual files,
 * whether single or multiple, whether auto-rotated, compacted, encrypted, etc.
 */
trait StorageReader {
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T
  def close: Unit
  
  // TODO: One day revisit adding NIO/file-channel or transmitTo methods.
}

trait StorageAppender {
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc
  def close: Unit
}

trait Storage extends StorageReader with StorageAppender {
  def storageLocSize: Int = 4 + 8 /* sizeof(int) + sizeof(long) */
}

// ---------------------------------------------------------

trait StorageLocReader {
  def readArray: Array[Byte]
  def readLoc: StorageLoc
  def readUTF: String
  def readByte: Byte
  def readShort: Short  
  def readInt: Int
  def readLong: Long
}

trait StorageLocAppender {
  def appendArray(arr: Array[Byte], offset: Int, len: Int): Unit
  def appendLoc(loc: StorageLoc): Unit
  def appendUTF(s: String): Unit
  def appendByte(s: Byte): Unit
  def appendShort(s: Short): Unit
  def appendInt(s: Int): Unit
  def appendLong(s: Long): Unit
  def flush: Unit
}

// ---------------------------------------------------------

/**
 * Immutable opaque pointer to location in storage.
 * The id is an opaque identifier to a storage shard, possibly 
 * representing a file.  The position is also opaque, possibly
 * representing a byte offset in a file.  
 */
case class StorageLoc(id: Int, position: Long)

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
      throw new RuntimeException("cannot overwrite an existing swizzle loc")
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
 * A simple storage reader that accesses a single file.
 */
class FileStorageReader(f: File) extends StorageReader {
  private val raf = new RandomAccessFile(f, "r")
  
  def close = synchronized { raf.close }
  
  private val reader = new StorageLocReader {
    def readArray: Array[Byte] = {
      val len = raf.readInt
      val arr = new Array[Byte](len)
      raf.readFully(arr)
      arr
    }
        
    def readLoc: StorageLoc = StorageLoc(raf.readInt, raf.readLong)
    def readUTF: String     = raf.readUTF
    def readByte: Byte      = raf.readByte
    def readShort: Short    = raf.readShort
    def readInt: Int        = raf.readInt
    def readLong: Long      = raf.readLong
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
 * A simple storage implementation that reads and appends to a single file.
 *
 * TODO: Need a separate sync/lock for read operations than for append operations.
 */
class FileStorage(f: File) extends FileStorageReader(f) with Storage {
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
      fosData.writeInt(loc.id)
      fosData.writeLong(loc.position)
    }
    
    def appendUTF(s: String): Unit  = fosData.writeUTF(s)
    def appendByte(s: Byte): Unit   = fosData.writeByte(s)
    def appendShort(s: Short): Unit = fosData.writeShort(s)
    def appendInt(s: Int): Unit     = fosData.writeInt(s)
    def appendLong(s: Long): Unit   = fosData.writeLong(s)
    
    def flush = {
      fosData.flush
      fosChannel.force(true) // TODO: Do we really need both flush and force?
    }
  }

  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc = 
    synchronized {
      val loc = StorageLoc(0, fosChannel.size)

      // We assume the callback func is not holding onto the appender parameter.
      //
      func(loc, appender)
      
      // Note: the flush keeps the fosChannel.size metadata correct.
      // TODO: is there a more efficient way to keep fosChannel.size correct?
      //
      appender.flush

      loc
    }
}

// ---------------------------------------------------------

/**
 * An append-only file with a header and a "permaMarker", which is a unique 
 * marker that signals a high-water point in the file.  All data to the left
 * or written before the permaMarker is stable.
 *
 * TODO: Should read the header, do version comparisons, etc.
 */
class FileWithPermaHeader(
        f: File, 
        headerLines: String, 
        permaMarkerDefault: Array[Byte]) {
  val sizeOfInt = 4

  def headerLength = 300
  def headerSuffix = (0 until (headerLength - headerLines.length)).map(x => "\n").mkString
  def header       = headerLines + headerSuffix

  def permaMarkerLength = permaMarkerDefault.length
  
  val permaMarker: Array[Byte] = 
    if (f.exists &&
        f.length >= (headerLength + sizeOfInt + permaMarkerLength).toLong)
      readHeaderPermaMarker
    else 
      initHeaderPermaMarker
  
  /**
   * Read permaMarker bytes from header area of existing file.
   */
  def readHeaderPermaMarker: Array[Byte] = {
    if (!f.isFile)
      throw new StorageException("not a file: " + f.getPath)
    if (!f.canRead)
      throw new StorageException("could not read file: " + f.getPath)

    val i = new DataInputStream(new FileInputStream(f))
    try {
      i.skipBytes(headerLength)
      val n = i.readInt
      if (n != permaMarkerLength)
        throw new StorageException("perma marker length mismatch: " + n + " in file: " + f.getPath)
      val m = new Array[Byte](n)
      i.read(m)
      m
    } finally {
      i.close
    }
  }
  
  /**
   * Create and emit header for a brand new file.
   */
  def initHeaderPermaMarker: Array[Byte] = {
    f.delete

    val o = new DataOutputStream(new FileOutputStream(f))
    try {
      o.write    (header.getBytes)
      o.writeInt (permaMarkerLength)  // Note: same as appender.appendArray format, of array length.
      o.write    (permaMarkerDefault) // Note: same as appender.appendArray format, of array body.
      o.flush
    } finally {
      o.close
    }
    permaMarkerDefault
  }
  
  /**
   * Scan backwards in storage for the last permaMarker.  Also, truncate file if found.
   */
  val initialPermaLoc: StorageLoc = scanForPermaMarker
  
  def scanForPermaMarker: StorageLoc = {
    val raf = new RandomAccessFile(f, "rws")
    try {
      val minimumPos = (headerLength + sizeOfInt).toLong

      val mArr = new Array[Byte](permaMarkerLength)
      var mPos = -1L
      var cPos = raf.length - permaMarkerLength.toLong
      while (mPos < 0L &&
             cPos >= minimumPos) {
        raf.seek(cPos)
        raf.read(mArr)
        if (mArr.deepEquals(permaMarker))
          mPos = cPos
        else
          cPos = cPos - 1L // TODO: Do a faster backwards scan.
      }

      if (mPos < minimumPos)
        throw new StorageException("could not find permaMarker in file: " + f.getPath)
        
      // Truncate the file, because everything after the last permaMarker
      // is a data write/append that got only partially written,
      // perhaps due to a crash or process termination.
      //
      raf.setLength(mPos + permaMarker.length.toLong)

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
}

// ---------------------------------------------------------

/**
 * A storage implementation that tracks multiple db log files in a directory,
 * appending to the most recent file, but reading from any file.  That is,
 * you can have pointers (aka, StorageLoc's) that point to any (active) 
 * log file in the directory.
 *
 * A log file name looks like "db_XXXXXXXX.log"
 * where XXXXXXXX is the id part in hexadecimal.
 *
 * TODO: Need a separate sync/lock for read operations than for append operations.
 */
abstract class DirStorage(subDir: File) extends Storage {
  def filePrefix    = "db_"
  def fileSuffix    = ".log" // An append-only db log file.
  def fileIdInitial = 0

  def defaultHeader: String
  def defaultPermaMarker: Array[Byte]
  
  // List of all the db files in the subDir when we first started,
  // highest numbered files first.
  //  
  val initialFileNames = subDir.list.toList.
                                filter(n => n.startsWith(filePrefix) &&
                                            n.endsWith(fileSuffix)).
                                sort(_ > _)

  val MANY_ZEROS = "00000000000000"
        
  def fileIdPart(id: Int): String = {           // Returns a string like "00000000", "00000123".
      val s = Integer.toHexString(id)
      MANY_ZEROS.substring(0, 8 - s.length) + s // Returned string is zero-perfixed to reach 8 chars.
  }

  def fileNameId(fileName: String): Int =
      Integer.parseInt(fileNameIdPart(fileName), 16)
    
  def fileNameIdPart(fileName: String): String =
      fileName.substring(filePrefix.length, fileName.indexOf("."))
      
  case class StorageInfo(fs: FileStorage, permaHeader: FileWithPermaHeader)

  private var currentStorages: immutable.SortedMap[Int, StorageInfo] = 
    openStorages(initialFileNames match {
                    case Nil => List(filePrefix + fileIdPart(fileIdInitial) + fileSuffix)
                    case xs  => xs
                 })

  def openStorages(fileNames: Seq[String]): immutable.SortedMap[Int, StorageInfo] =
    immutable.TreeMap[Int, StorageInfo](
      fileNames.map(
        fileName => {
          val f  = new File(subDir + "/" + fileName)
          val ph = new FileWithPermaHeader(f, defaultHeader, defaultPermaMarker)
          val fs = new FileStorage(f) // Note: we create fs after ph, because ph has initialization code.

          Pair(fileNameId(fileName), StorageInfo(fs, ph))
        }
      ):_*)

  // TODO: Should use FileStorageReader's (read-only) for old files and 
  // use FileStorage (read & append) for only the most recent/active file.
  //
  def currentStorageId: Int    = synchronized { currentStorages }.lastKey
  def storageInfo: StorageInfo = synchronized { currentStorages(currentStorageId) }

  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T         = storageInfo.fs.readAt(loc, func)
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc = storageInfo.fs.append(func)
  
  val initialPermaLoc: StorageLoc = 
    (currentStorages.map(x => x._2.permaHeader.initialPermaLoc). // Scan backwards thru log files.
                     filter(_ != StorageLoc(-1, -1L)).           // Find the first good permaLoc.
                     toList ::: (StorageLoc(-1, -1L) :: Nil)).head
  
  def permaMarker: Array[Byte] = storageInfo.permaHeader.permaMarker
      
  def close: Unit = 
    synchronized {
      for ((id, si) <- currentStorages) // TODO: Race condition in close with in-flight reads/appends.
        si.fs.close                     // TODO: Need to wait for current ops to finish first?
      currentStorages = currentStorages.empty
    }
}

