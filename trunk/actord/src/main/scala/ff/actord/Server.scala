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

import scala.collection._

import ff.actord.Util._

object MServer {
  def version = "actord-0.1.0"
}

// --------------------------------------------

trait MServer {
  def subServerList: List[MSubServer]
  
  def get(keys: Seq[String]): Iterator[MEntry]
  def set(el: MEntry, async: Boolean): Boolean
  def delete(key: String, time: Long, async: Boolean): Boolean

  /**
   * A transport protocol can convert incoming incr/decr messages to delta calls.
   */
  def delta(key: String, mod: Long, async: Boolean): Long
    
  /**
   * For add or replace.
   */
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean): Boolean

  /**
   * A transport protocol can convert incoming append/prepend messages to xpend calls.
   */
  def xpend(el: MEntry, append: Boolean, async: Boolean): Boolean

  /**
   * For CAS mutation.
   */  
  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean): String

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String]
  
  def flushAll(expTime: Long): Unit
  
  def stats: MServerStats

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry]

  def act(el: MEntry, async: Boolean): Iterator[MEntry]
}

// --------------------------------------------

/**
 * Tracks key/value entries and their LRU (least-recently-used) history.
 *
 * We use immutable tree structures for better concurrent multi-core 
 * read/query/get performance, and for poor man's MVCC.  
 *
 * However, the cost might be slower write/set performance.  
 * For higher write multi-core concurrency, we internally shard 
 * the key/value data into separate MSubServer instances, with 
 * usually one MSubServer per CPU processor.  All write/set operations 
 * within a MSubServer instance are serialized, optionally asynchronously,
 * behind an per-MSubServer-actor which handles all modifications for that
 * MSubServer instance.  
 *
 * We ideally want the MServer/MSubServer classes to be 
 * independent of transport or wire protocol.  So grep'ing this 
 * file should produce zero transport/wire dependencies.
 */
class MMainServer(val subServerNum: Int, // Number of internal "shards" for this server.
                  val limitMemory: Long) // Measured in bytes.
  extends MServer {
  def this() = this(Runtime.getRuntime.availableProcessors, 0L)
  
  val createdAt = System.currentTimeMillis

  /**
   * Track an array of MSubServers, meant to locally "shard" the 
   * key/value data across multiple cores.
   */
  protected val subServers = new Array[MSubServer](subServerNum)
  for (i <- 0 until subServerNum)
    subServers(i) = createSubServer(i)
    
  def subServerList = subServers.toList
  
  /**
   * Subclasses might override to provide a custom MSubServer.
   */
  def createSubServer(id: Int): MSubServer = 
    new MSubServer(id, limitMemory / subServerNum)
  
  def subServerForKey(key: String) = 
    if (subServerNum <= 1)
        subServers(0)
    else
        subServers(subServerIdForKey(key))

  /**
   * Subclasses might override to provide a better hashing.
   */
  def subServerIdForKey(key: String) = Math.abs(key.hashCode) % subServerNum
  
  // --------------------------------------------------

  /**
   * Group the keys into the buckets to efficiently send to the right subServer.
   * Note that the ordering of the result Iterator[MEntry] will not follow the 
   * ordering of the input keys.
   */  
  def get(keys: Seq[String]): Iterator[MEntry] = {
    if (subServerNum <= 1) 
        subServers(0).get(keys)
    else {
      // First group the keys for each subServer, for better 
      // cache locality and synchronization avoidance.
      //
      val groupedKeys = new Array[mutable.ArrayBuffer[String]](subServerNum) 
      for (i <- 0 until subServerNum)
        groupedKeys(i) = new mutable.ArrayBuffer[String]

      for (key <- keys) {
        val i = subServerIdForKey(key)
        groupedKeys(i) += key
      }
    
      val empty: Iterator[MEntry] = Iterator.empty
    
      (0 until subServerNum).
        foldLeft(empty)((result, i) => result.append(subServers(i).get(groupedKeys(i))))
    }
  }

  def set(el: MEntry, async: Boolean) = 
    subServerForKey(el.key).set(el, async)

  def delete(key: String, time: Long, async: Boolean) = 
    subServerForKey(key).delete(key, time, async)

  def delta(key: String, mod: Long, async: Boolean): Long =
    subServerForKey(key).delta(key, mod, async)
    
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean) = 
    subServerForKey(el.key).addRep(el, isAdd, async)

  def xpend(el: MEntry, append: Boolean, async: Boolean) =
    subServerForKey(el.key).xpend(el, append, async)
  
  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean) =
    subServerForKey(el.key).checkAndSet(el, cidPrev, async)

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String] = {
    val empty = List[String]().elements
    subServers.foldLeft(empty)((accum, next) => next.keys.append(accum))
  }
  
  def flushAll(expTime: Long) = subServers.foreach(_.flushAll(expTime))
  
  def stats = 
    subServers.foldLeft(new MServerStats)((accum, subServer) => accum + subServer.stats)

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    val empty: Iterator[MEntry] = Iterator.empty 
    subServers.foldLeft(empty)((result, s) => result.append(s.range(keyFrom, keyTo)))
  }

  def act(el: MEntry, async: Boolean) = Iterator.empty
}

// --------------------------------------------

case class MServerStatsRequest
case class MServerStats(numEntries: Long,
                        usedMemory: Long,
                        evictions: Long,
                        cmd_gets: Long, 
                        cmd_sets: Long,
                        get_hits: Long,
                        get_misses: Long,
                        lruSize: Long) {
  def this() = this(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
  def +(that: MServerStats) =
    MServerStats(numEntries + that.numEntries, 
                 usedMemory + that.usedMemory, 
                 evictions  + that.evictions,
                 cmd_gets   + that.cmd_gets, 
                 cmd_sets   + that.cmd_sets,
                 get_hits   + that.get_hits,
                 get_misses + that.get_misses,
                 lruSize    + that.lruSize)
}

// -------------------------------------------------------

case class MEntry(key: String, 
                  flags: Long,
                  expTime: Long,     // Expiry timestamp, in seconds since epoch.
                  data: Array[Byte],
                  cid: Long) {       // Unique id for CAS operations.
  def isExpired: Boolean = 
      isExpired(nowInSeconds)
  
  def isExpired(now: Long): Boolean = 
    (key == null) || // A null key means an error entry, possibly from unreadable files.
    (expTime != 0L &&
     expTime < now)

  // TODO: Revisit the cid + 1L update, as concurrent threads could
  //       generate the same updated cid number.  Not sure if that's
  //       a problem.
  // 
  def updateExpTime(e: Long) =
    MEntry(key, flags, e, data, cid + 1L)

  def updateData(d: Array[Byte]) =
    MEntry(key, flags, expTime, d, cid + 1L)
  
  /**
   * Concatenate the data arrays from this with that,
   * using the basis for all other fields.
   */
  def concat(that: MEntry, basis: MEntry) = {
    val sizeNew = this.data.size + that.data.size
    val dataNew = new Array[Byte](sizeNew)

    System.arraycopy(this.data, 0, dataNew, 0,              this.data.size)
    System.arraycopy(that.data, 0, dataNew, this.data.size, that.data.size)    
    
    MEntry(basis.key, 
           basis.flags, 
           basis.expTime,
           dataNew,
           basis.cid + 1L)
  }

  /**
   * Only the MSubServer mod actor should read or use the lru field.
   * Note that we don't copy the lru field during updateXxx() or concat()
   * so that the mod actor can do proper stats accounting.
   */  
  var lru: LRUList = null

  /**
   * Networking/communication layers can squirrel away memeoized data here.
   */
  private var commObj: AnyRef = null

  def comm                      = synchronized { commObj }
  def comm_!(a: AnyRef): AnyRef = synchronized { commObj = a; a }
}

// -------------------------------------------------------

class LRUList(var elem: String,
              var next: LRUList,
              var prev: LRUList) 
  extends mutable.DoubleLinkedList[String, LRUList] {
  def key = elem
}

