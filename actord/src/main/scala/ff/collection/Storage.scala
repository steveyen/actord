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
trait Storage extends StorageReader with StorageAppender {
  def storageLocSize: Int = 4 + 8 /* sizeof(int) + sizeof(long) */
  
  /**
   * Return true if a given loc should be re-saved again,
   * since the loc might point to an valid, but ancient location.
   * That is, the loc might point to an old file that needs to be compacted.
   *
   * Subclasses that implement multiple, rotated storage files 
   * can override this method to provide cleanup/vacuum behavior.
   */
  def storageLocRefresh(loc: StorageLoc): Boolean = false
}

trait StorageReader {
  /**
   * Clients can call readAt, providing a loc and a func/callback.
   * The func/callback is passed a managed StorageLocReader that is
   * good only for the duration of the func/callback invocation.
   * The managed StorageLocReader provides access to the bytes at the required loc.
   */
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T
  def close: Unit
  
  // TODO: One day revisit adding NIO/file-channel or transmitTo methods.
}

trait StorageAppender {
  /**
   * Clients can call append, providing a func/callback.
   * The func/callback is passed a StorageLoc and a managed StorageLocAppender.
   * The StorageLocAppender instance is good only for the duration of the 
   * func/callback invocation.  The StorageLoc points to the location 
   * where any bytes will be written/appended, and can be saved or held
   * by the func/callback code for a future StorageReader#readAt call.
   */
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc
  def close: Unit
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
 *
 * Some literature calls this a LSN or log sequence number.
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
 * - has both in-memory value and storage location.
 */
class StorageSwizzle[S <: AnyRef] {
  protected var value_i: S        = _
  protected var loc_i: StorageLoc = null

  def value: S       = valueSync.synchronized { value_i }
  def value_!!(x: S) = valueSync.synchronized { 
    if (x != null && value_i != null)
      throw new RuntimeException("cannot overwrite an existing swizzle value")
    value_i = x
    value_i
  }
  
  def loc: StorageLoc       = locSync.synchronized { loc_i }
  def loc_!!(x: StorageLoc) = locSync.synchronized { 
    if (x != null && loc_i != null)
      throw new RuntimeException("cannot overwrite an existing swizzle loc")
    loc_i = x
    loc_i 
  }

  // We have separate sync monitors for the loc and value slots,
  // for more concurrency.
  //
  // Be careful about deadlocks, though, during nested locking code.
  //  
  def valueSync = this
  val locSync   = new Object
}

// ---------------------------------------------------------

class RAInputStream(raf: RandomAccessFile) extends InputStream {
  override def read                                     = raf.read
  override def read(b: Array[Byte])                     = raf.read(b)
  override def read(b: Array[Byte], off: Int, len: Int) = raf.read(b, off, len)
}

// ---------------------------------------------------------

/**
 * A simple storage reader that accesses a single file.
 */
class FileStorageReader(f: File, id: Int) extends StorageReader {
  def this(f: File) = this(f, 0)
  
  protected val raf = new RandomAccessFile(f, "r") 

  protected val ris: RAInputStream   = new RAInputStream(raf)
  protected var dis: DataInputStream = null
  
  def close = synchronized { raf.close }
  
  protected val reader = new StorageLocReader {
    def readArray: Array[Byte] = {
      val len = dis.readInt
      val arr = new Array[Byte](len)
      dis.readFully(arr)
      arr
    }
        
    def readLoc: StorageLoc = StorageLoc(dis.readInt, dis.readLong)
    def readUTF: String     = dis.readUTF
    def readByte: Byte      = dis.readByte
    def readShort: Short    = dis.readShort
    def readInt: Int        = dis.readInt
    def readLong: Long      = dis.readLong
  }
  
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T = {
    checkLoc(loc)

    synchronized {
      raf.seek(loc.position)
      dis = new DataInputStream(ris) // TODO: Reduce throwaway garbage.
      val result = func(reader)
      dis = null
      result
    }
  }
  
  def checkLoc(loc: StorageLoc) = {
    if (loc == null)
      throw new RuntimeException("bad loc during SFS readAt: " + loc)
    if (loc.id != id)
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
class FileStorage(f: File, id: Int) extends FileStorageReader(f, id) with Storage {
  def this(f: File) = this(f, 0)
  
  protected val fos        = new FileOutputStream(f, true)
  protected val fosData    = new DataOutputStream(new BufferedOutputStream(fos))
  protected val fosChannel = fos.getChannel
  
  override def close = synchronized {
    appender.flush
    fosData.close
    super.close
  }

  protected val appender = new StorageLocAppender {
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
      val loc = StorageLoc(id, fosChannel.size)

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
 * TODO: Should read the header, have callbacks to handle old versions/formats, etc.
 */
class FileWithPermaMarkerHeader(f: File, 
                                id: Int,
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
   * Create and emit header and permaMarker bytes for a brand new file.
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
   * Scan backwards in storage for the last permaMarker.  Optionally truncate file if found.
   */
  def scanForPermaMarker(truncate: Boolean): StorageLoc = {
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
      if (truncate)
        raf.setLength(mPos + permaMarker.length.toLong)

      // Negative loc values means it's a clean, just-initialized file.
      //
      if (mPos == minimumPos)
        StorageLoc(-1, -1L)
      else
        StorageLoc(id, mPos - sizeOfInt.toLong)
    } finally {
      raf.close
    }
  }
}

// ---------------------------------------------------------

trait StorageWithPermaMarker extends Storage {
  def appendWithPermaMarker(func: (StorageLoc, StorageLocAppender, Array[Byte]) => Unit): StorageLoc
  def initialPermaMarkerLoc: StorageLoc
}

// ---------------------------------------------------------

/**
 * A storage implementation that tracks multiple db log files in a directory,
 * appending to the most recent file, but reading from any active log file.  
 * That is, you can have pointers (aka, StorageLoc's) that point to any 
 * active log file in the directory.
 *
 * A log file name looks like "db_XXXXXXXX.log"
 * where XXXXXXXX is the id part in hexadecimal.
 *
 * TODO: Need a separate sync/lock for read operations than for append operations.
 */
abstract class DirStorage(subDir: File) extends StorageWithPermaMarker {
  def filePrefix    = "db_"
  def fileSuffix    = ".log" // An append-only db log file.
  def fileIdInitial = 0

  def defaultHeader: String
  def defaultPermaMarker: Array[Byte]
  
  // List of all the db files in the subDir when we first started,
  // with files of highest id sorted first.
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
      
  // TODO: Should use FileStorageReader's (read-only) for old files and 
  // use FileStorage (read & append) for only the most recent/active file.
  //
  case class FileInfo(fs: FileStorage, permaMarkerHeader: FileWithPermaMarkerHeader)

  protected var currentFiles: immutable.SortedMap[Int, FileInfo] = 
    openFiles(initialFileNames match {
                   case Nil => List(filePrefix + fileIdPart(fileIdInitial) + fileSuffix)
                   case xs  => xs
                 })

  def openFiles(fileNames: Seq[String]): immutable.SortedMap[Int, FileInfo] =
    immutable.TreeMap[Int, FileInfo](
      fileNames.map(fileName => Pair(fileNameId(fileName), openFile(fileName))):_*)
      
  def openFile(fileName: String) = {
    val id = fileNameId(fileName)
    val f  = new File(subDir + "/" + fileName)
    val ph = new FileWithPermaMarkerHeader(f, id, defaultHeader, defaultPermaMarker)
    val fs = new FileStorage(f, id) // Note: we create ph before fs, because ph has initialization code.
    FileInfo(fs, ph)
  }
  
  def pushNextFile: Int =
    synchronized {
      val nextFileId   = Math.max(0, currentFileId + 1)
      val nextFileName = filePrefix + fileIdPart(nextFileId) + fileSuffix
      currentFiles     = currentFiles + (nextFileId -> openFile(nextFileName))
      nextFileId
    }
    
  protected def currentFileId: Int = synchronized { currentFiles }.lastKey

  /**
   * Returns the FileInfo for the current (newest) appendable log file.
   */  
  protected def fileInfo: FileInfo = {
    val cs = synchronized { currentFiles }
    
    // Since currentFiles is immutable, we can traverse outside the synchronized.
    //
    cs(cs.lastKey)
  }

  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T = {
    val cs = synchronized { currentFiles }
    
    // Since currentFiles is immutable, we can traverse outside the synchronized.
    //
    cs(loc.id).fs.readAt(loc, func)
  }
  
  // TODO: Check the logic to make sure append never gets splits across 
  //       two files, such as if pushNextFile is called concurrently during append.
  //
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc = fileInfo.fs.append(func)

  def appendWithPermaMarker(func: (StorageLoc, StorageLocAppender, Array[Byte]) => Unit): StorageLoc = {
    val fi = fileInfo 
    
    // We grab a snapshot of fileInfo above to avoid race conditions, so
    // that we have the right permaMarker associated with the right appender,
    // and can pass it all together to the callback worker func.
    //
    fi.fs.append((loc, appender) => func(loc, appender, fi.permaMarkerHeader.permaMarker))
  }
  
  val initialPermaMarkerLoc: StorageLoc = 
    (currentFiles.map(x => x._2.permaMarkerHeader.scanForPermaMarker(true)). // Scan backwards thru log files, 
                  filter(_ != StorageLoc(-1, -1L)).                          // and, find the largest good permaMarkerLoc.
                  toList.
                  sort(_.id > _.id) ::: (StorageLoc(-1, -1L) :: Nil)).head
  
  def close: Unit = 
    synchronized {
      for ((id, si) <- currentFiles) // TODO: Race condition in close with in-flight reads/appends.
        si.fs.close                     // TODO: Need to wait for current ops to finish first?
      currentFiles = currentFiles.empty
    }
}

