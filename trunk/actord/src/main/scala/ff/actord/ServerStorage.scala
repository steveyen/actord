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

class MSubServerStorage(subDir: File) extends Storage {
  // A file name looks like db_XXXXXXXX.[log|chk], 
  // where XXXXXXXX is the id part of the file name.
  //
  // A log file and checkPoint file have the same file format.
  //
  val filePrefix    = "db_"
  val fileSuffixLog = ".log" // A append-only log file.
  val fileSuffixChk = ".cpt" // A compacted checkPoint file.

  // List of all the db files in the subDir when we first started,
  // lowest numbered files first.
  //  
  val initialFileNames = subDir.list.toList.
                                filter(n => n.startsWith(filePrefix)).
                                filter(n => n.endsWith(fileSuffixLog) ||
                                            n.endsWith(fileSuffixChk)).
                                sort(_ < _)

  // Group the initialFileNames by their checkPoint files, with
  // highest numbers (most recent) coming first.
  // 
  // For example....
  //
  //   List(db_000.log, db_001.cpt, db_002.log, db_003.log, db_004.cpt, db_005.log, db_006.log)
  //
  // Should be grouped like...
  //
  //   Pair(Some("db_004.cpt"), List("db_006.log", "db_005.log")] ::
  //   Pair(Some("db_001.cpt"), List("db_003.log", "db_002.log")] ::
  //   Pair(None,               List("db_000.log")] ::
  //   Nil
  //
  val initialFileNameGroups: List[Pair[Option[String], List[String]]] = 
      initialFileNames.foldLeft[List[Pair[Option[String], List[String]]]](Nil)(
        (accum, nextFileName) =>
          if (nextFileName.endsWith(fileSuffixChk))
            Pair(Some(nextFileName), Nil) :: accum
          else {
            accum match {
              case group :: xs =>
                Pair(group._1, nextFileName :: group._2) :: xs
              case Nil =>
                Pair(None, List(nextFileName)) :: Nil
            }
          }
        ) 

  def fileId(fileName: String): Short =
      fileIdPart(fileName).toShort
    
  def fileIdPart(fileName: String): String =
      fileName.substring(filePrefix.length, fileName.indexOf("."))

  private var currentStorages: immutable.SortedMap[Short, SingleFileStorage] = 
    openStorages(initialFileNameGroups.headOption.
                                       map(group => group._2.concat(group._1.toList).toList).
                                       getOrElse(Nil))

  def openStorages(fileNames: Seq[String]): immutable.SortedMap[Short, SingleFileStorage] =
    immutable.TreeMap[Short, SingleFileStorage](
      fileNames.map(fileName => 
                      Pair(fileId(fileName), 
                           new SingleFileStorage(new File(subDir + "/" + fileName)))):_*)

  var currentStorageId: Short = 
    synchronized { currentStorages }.lastKey
  
  def readAt[T](loc: StorageLoc, func: StorageLocReader => T): T = 
    synchronized { currentStorages(loc.id) }.readAt(loc, func)
  
  def append(func: (StorageLoc, StorageLocAppender) => Unit): StorageLoc =
    synchronized { currentStorages(currentStorageId) }.append(func)
  
  def close: Unit = 
    synchronized {
      for ((id, s) <- currentStorages) // TODO: Race condition in close with in-flight reads/appends.
        s.close                        // TODO: Need to wait for current ops to finish first?
      currentStorages = currentStorages.empty
    }

  /**
   * A checkPoint collapses all current logs and the previous checkPoint file (if any)
   * into a brand new checkPoint file.  After that, those log and previous 
   * checkPoint files can be deleted.  New log files will point to the 
   * new checkPoint file and to new log files.
   */
  def checkPoint: Unit = {
    // see if temp checkPoint file exists
    //   that means another process is doing checkpointing -- not supposed to happen.
    // make temp checkPoint file
    // write checkPoint to temp
    // rename to actual checkpoint file
    // all writes are stopped while this is happening?
    //   better would be if writes are just slowed down
  }
  
  /**
   * Start a new log file.
   */
  def logFilePush: Unit = 
    synchronized {
      val nextLogFileId = Math.max(0, currentStorageId + 1)
      nextLogFileId
    }

  // TODO: File rotation.
  //
  val f = new File(subDir + "/db_00000000.data")
  
  def HEADER_LENGTH = 300
  def HEADER_LINE   = "# actord data file, format: binary-0.0.1\n\n"
  def HEADER_SUFFIX = (0 until (HEADER_LENGTH - HEADER_LINE.length)).map(x => "\n").mkString
  def HEADER        = HEADER_LINE + HEADER_SUFFIX

  def PERMA_DEFAULT = "a#Fq9a2b3Kh5sYf8x001".getBytes // Sort of like a anchor/checkpoint marker.
  def PERMA_LENGTH  = PERMA_DEFAULT.length            // Everything before the PERMA_MARKER is stable.
  
  val PERMA_MARKER: Array[Byte] = {
    if (f.exists &&
        f.length >= (HEADER_LENGTH + PERMA_LENGTH).toLong) {
      // Read PERMA_MARKER from header area of existing file.
      //
      if (!f.isFile)
        throw new MStorageException("not a file: " + f.getPath)
      if (!f.canRead)
        throw new MStorageException("could not read file: " + f.getPath)

      val i = new DataInputStream(new FileInputStream(f))
      try {
        i.skipBytes(HEADER_LENGTH)
        val n = i.readInt
        if (n != PERMA_LENGTH)
          throw new MStorageException("perma marker length mismatch: " + n + " in file: " + f.getPath)
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
        o.writeInt(PERMA_LENGTH) // Note: same as appender.appendArray format, of array length.
        o.write(PERMA_DEFAULT)   // Note: same as appender.appendArray format, of array body.
        o.flush
      } finally {
        o.close
      }
      PERMA_DEFAULT
    }
  }
  
  val initialPermaLoc: StorageLoc = {
    // Scan backwards for the last PERMA_MARKER.  Also, truncate file if found.
    //
    // TODO: Handle multi-file truncation during backwards scan.
    //
    val raf = new RandomAccessFile(f, "rws")
    try {
      val sizeOfInt  = 4
      val minimumPos = (HEADER_LENGTH + sizeOfInt).toLong

      val mArr = new Array[Byte](PERMA_LENGTH)
      var mPos = -1L
      var cPos = raf.length - PERMA_LENGTH.toLong
      while (mPos < 0L &&
             cPos >= minimumPos) {
        raf.seek(cPos)
        raf.read(mArr)
        if (mArr.deepEquals(PERMA_MARKER))
          mPos = cPos
        else
          cPos = cPos - 1L // TODO: Do a faster backwards scan.
      }

      if (mPos < minimumPos)
        throw new MStorageException("could not find PERMA_MARKER in file: " + f.getPath)
        
      // Truncate the file, because everything after the last PERMA_MARKER
      // is a data write/append that got only partially written,
      // perhaps due to a crash or process termination.
      //
      raf.setLength(mPos + PERMA_MARKER.length.toLong)

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
      val locSize  = io.storageLocSize
      val locPerma = ss.initialPermaLoc
      if (locPerma.id >= 0 &&
          locPerma.position > locSize) {
        val locRoot = io.readAt(StorageLoc(locPerma.id, locPerma.position - locSize), _.readLoc)
        
        new MEntryTreapStorable(t.loadNodeAt(locRoot, None), io)
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
                      appender.appendArray(ss.PERMA_MARKER, 0, ss.PERMA_MARKER.length)
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

