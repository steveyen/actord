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

class MSubServerStorage(subDir: File) extends DirStorage(subDir) {
  def defaultHeader      = "# actord data file, format: binary-0.0.1\n\n"
  def defaultPermaMarker = "a#Fq9a2b3Kh5sYf8x001".getBytes
}

// ------------------------------------------------

class MPersistentSubServer(override val id: Int, 
                           override val limitMemory: Long, 
                           val subServerStorage: MSubServerStorage)
  extends MSubServer(id, limitMemory) {
  override def createSortedMap: immutable.SortedMap[String, MEntry] = {
    val t = new MEntryTreapStorable(TreapEmptyNode[String, MEntry], subServerStorage)
    
    // If the storage has a treap root, load it.
    //
    // TODO: What about file versioning?
    //
    val locSize  = subServerStorage.storageLocSize
    val locPerma = subServerStorage.initialPermaLoc
    if (locPerma.id >= 0 &&
        locPerma.position > locSize) {
      val locRoot = subServerStorage.readAt(StorageLoc(locPerma.id, locPerma.position - locSize), _.readLoc)
      
      new MEntryTreapStorable(t.loadNodeAt(locRoot, None), subServerStorage)
    } else
      t
  }
    
  override protected def data_i_!!(d: immutable.SortedMap[String, MEntry]) = 
    synchronized { 
      super.data_i_!!(d) 
      version_i = Math.max(0L, version_i + 1L)
    }

  /**
   * A transient counter of how many times the data has changed in memory.
   */
  protected var version_i: Long = 0L
  def version: Long = synchronized { version_i }
  
  def dataWithVersion = 
    synchronized {
      Pair(data, version)
    }
  
  /**
   * Which transient version number/counter was last completely 
   * saved or persisted to nonvolatile storage.
   */
  protected var lastPersistedVersion_i: Long = -1L
  
  def lastPersistedVersion_!!(v: Long) = synchronized { lastPersistedVersion_i = v }
  def lastPersistedVersion: Long       = synchronized { lastPersistedVersion_i }

  /**
   * Which transient version number/counter was last completely cleaned.
   */
  protected var lastCleanedVersion_i: Long = -1L
  
  def lastCleanedVersion_!!(v: Long) = synchronized { lastCleanedVersion_i = v }
  def lastCleanedVersion: Long       = synchronized { lastCleanedVersion_i }
}
  
// ------------------------------------------------

class MEntryTreapStorable(override val root: TreapNode[String, MEntry],
                          val subServerStorage: MSubServerStorage)
  extends TreapStorable[String, MEntry](root, subServerStorage) {
  override def mkTreap(r: TreapNode[String, MEntry]): Treap[String, MEntry] = 
    new MEntryTreapStorable(r, subServerStorage)    
  
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
}

// ------------------------------------------------

class MPersister(subServersIn: Seq[MSubServer], // The subServers that this persister will manage.
                 checkInterval: Int,            // In millisecs, interval to check for dirty data.
                 limitFileSize: Long)           // In bytes, max size for each db log file.
  extends Runnable {
  def run { 
    val subServers = subServersIn.map(_.asInstanceOf[MPersistentSubServer])
  
    while (true) {
      val beg = System.currentTimeMillis
      
      for (subServer <- subServers) {
        val (d, v) = subServer.dataWithVersion
        if (v != subServer.lastPersistedVersion)
          d match {
            case currTreap: MEntryTreapStorable => 
              if (currTreap.subServerStorage == subServer.subServerStorage) {
                val locRoot = currTreap.appendNode(currTreap.root)
                
                currTreap.subServerStorage.appendWithPermaMarker((loc, appender, permaMarker) => {
                  appender.appendLoc(locRoot)
                  appender.appendArray(permaMarker, 0, permaMarker.length)
                })
    
                subServer.lastPersistedVersion_!!(v)
                
                if (locRoot.position >= limitFileSize)
                  currTreap.subServerStorage.pushNextFile
              }
          }
      }
        
      val end = System.currentTimeMillis
      val amt = checkInterval - (end - beg)
      if (amt > 0)
        Thread.sleep(amt)
    }
  } 
}

// ------------------------------------------------

class MCleaner(subServersIn: Seq[MSubServer], // The subServers that this cleaner will manage.
               checkInterval: Int)            // In millisecs, interval to run cleaning.
  extends Runnable {
  def run { 
    val subServers = subServersIn.map(_.asInstanceOf[MPersistentSubServer])
  
    while (true) {
      val beg = System.currentTimeMillis
      
      for (subServer <- subServers) {
        val (d, v) = subServer.dataWithVersion
        if (v != subServer.lastCleanedVersion)
          d match {
            case currTreap: MEntryTreapStorable => 
              if (currTreap.subServerStorage == subServer.subServerStorage) {
                walk(currTreap, 0, currTreap.root)           
                subServer.lastCleanedVersion_!!(v)
              }
          }
      }
        
      val end = System.currentTimeMillis
      val amt = checkInterval - (end - beg)
      if (amt > 0)
        Thread.sleep(amt)
    }
  } 
  
  def walk(treap: MEntryTreapStorable, minFileId: Int, node: TreapNode[String, MEntry]): Unit =
    node match {
      case e: TreapEmptyNode[String, MEntry] =>
      case x: TreapStorableNode[String, MEntry] =>
        val loc = x.swizzleValue.loc
        if (loc != null &&
            loc.id < minFileId) {
            val vLoaded = x.swizzleValue.value != null
            val v       = x.value // Forces a load, if not already loaded.
            
            x.swizzleValue.loc_!!(null)
            
            if (vLoaded == false)
              x.swizzleValue.value_!!(null)
        }
      
        val lLoaded = x.swizzleLeft.value != null
        walk(treap, minFileId, x.left)
        if (lLoaded == false &&
            x.swizzleLeft.loc != null)
            x.swizzleLeft.value_!!(null) // TODO: Possible concurrent error here?
        
        val rLoaded = x.swizzleRight.value != null
        walk(treap, minFileId, x.right)
        if (rLoaded == false &&
            x.swizzleRight.loc != null)
            x.swizzleRight.value_!!(null) // TODO: Possible concurrent error here?
    }
  
    // TODO: Implement cleaner/compacter.
    //
    // Walk through treap nodes, looking at swizzle objects.
    // If swizzle value loc != null && loc.id < minFileId
    //   If swizzle value value not loaded, load it from old file.
    //   Save swizzle value value to new loc in most recent file.
    //   If we (the cleaner) loaded the value, then unlink it from memory for GC.
    // Got to make sure concurrent appenders aren't saving locs that point to old files.
    //
    // If swizzle self loc < minFileId
    //   If swizzle self value not loaded, load it from old file.
    //   Save swizzle self value to new loc in most recent file.
    //   If we (the cleaner) loaded the self value, then unlink it from memory for GC.
    // Got to make sure concurrent appenders aren't saving locs that point to old files.
    //
    // Recurse on node left and right
}

