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
import scala.actors._
import scala.actors.Actor._

import ff.actord.Util._

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
 * independent of transport or wire protocol.
 */
class MServer(val subServerNum: Int,   // Number of internal "shards" for this server.
              val limitMemory: Long) { // Measured in bytes.
  def this() = this(Runtime.getRuntime.availableProcessors, 0L)
  
  val createdAt = System.currentTimeMillis
  def version   = "actord-0.0.0"
  
  private val subServers = new Array[MSubServer](subServerNum)
  for (i <- 0 until subServerNum)
    subServers(i) = createSubServer(i)
    
  def createSubServer(id: Int): MSubServer = // For overridability.
    new MSubServer(id, limitMemory / subServerNum)
  
  def subServerForKey(key: String) = 
    if (subServerNum <= 1)
        subServers(0)
    else
        subServers(subServerIdForKey(key))
        
  def subServerIdForKey(key: String) = key.hashCode % subServerNum   
        
  def getMulti(keys: Array[String], out: (MEntry) => Unit): Unit = {
    // First group the keys for each subServer, for better 
    // cache locality and synchronization avoidance.
    //
    val groupedKeys = new Array[List[String]](subServerNum) 
    for (i <- 0 until subServerNum)
      groupedKeys(i) = Nil

    for (key <- keys) {
      val i = subServerIdForKey(key)
      groupedKeys(i) = key :: groupedKeys(i)
    }

    for (i <- 0 until subServerNum)
      subServers(i).getMulti(groupedKeys(i), out)
  }

  def get(key: String): Option[MEntry] =
    subServerForKey(key).get(key)
	
  def set(el: MEntry, async: Boolean) = 
    subServerForKey(el.key).set(el, async)

  def add(el: MEntry, async: Boolean) = 
    subServerForKey(el.key).add(el, async)

  def replace(el: MEntry, async: Boolean) = 
    subServerForKey(el.key).replace(el, async)

  def delete(key: String, time: Long, async: Boolean) = 
    subServerForKey(key).delete(key, time, async)
    
	def delta(key: String, mod: Long, async: Boolean): Long =
    subServerForKey(key).delta(key, mod, async)
    
  def append(el: MEntry, async: Boolean) =
    subServerForKey(el.key).append(el, async)
  
  def prepend(el: MEntry, async: Boolean) =
    subServerForKey(el.key).prepend(el, async)

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
	
	def stats = {
	  val empty = MServerStats(0L, 0L, 0L)
	  subServers.foldLeft(empty)((accum, subServer) => accum + subServer.stats)
	}
}

// --------------------------------------------

class MSubServer(val id: Int, val limitMemory: Long) {
  /**
   * Override to pass in other implementations, such as storage.SMap for persistence.
   */
  def createSortedMap: immutable.SortedMap[String, MEntry] =
                   new immutable.TreeMap[String, MEntry]

  /**
   * TODO: Maybe just use volatile, or AtomicReference around data_i.
   */
  private var data_i = createSortedMap
  
  private def data_i_!!(d: immutable.SortedMap[String, MEntry]) = 
    synchronized { data_i = d }  
  
  def data = synchronized { data_i } // Allows threads to snapshot the tree's root.
  
  // --------------------------------------------

  def getUnexpired(key: String): Option[MEntry] =
      getUnexpired(key, data)
    
  def getUnexpired(key: String, d: immutable.SortedMap[String, MEntry]): Option[MEntry] =
    d.get(key) match {
      case s @ Some(el) => {
        if (el.isExpired) {
          mod ! ModDelete(el, true) // Queue delete of expired entry.
          None 
        } else 
          s
      }
      case None => None
    }

  def getMulti(keys: List[String], out: (MEntry) => Unit): Unit = {
    var els: List[MEntry] = Nil

    // Grab the data snapshot just once, outside the loop.
    //
    val d = data 

    for (key <- keys)
      getUnexpired(key, d) match {
        case Some(el) => 
          els = el :: els
          out(el)
        case None =>
      }

    if (!els.isEmpty)
      mod ! ModTouch(els, true)
  }

  def get(key: String): Option[MEntry] =
    getUnexpired(key) match {
      case x @ Some(el) => mod ! ModTouch(List(el), true); x
      case x @ None => x
    }
	
  def set(el: MEntry, async: Boolean) = {
    if (async)
      mod ! ModSet(el, async)
    else
      mod !? ModSet(el, async)
    true
	}

  def add(el: MEntry, async: Boolean) = 
    getUnexpired(el.key) match {
      case Some(_) => false
      case None => set(el, async)
    }

  def replace(el: MEntry, async: Boolean) = 
    getUnexpired(el.key) match {
      case Some(_) => set(el, async)
      case None => false
    }

  def delete(key: String, time: Long, async: Boolean) = 
		getUnexpired(key).map(
		  el => {
		    if (time != 0L) {
          if (el.expTime == 0L || 
              el.expTime > (nowInSeconds + time)) 
              set(el.updateExpTime(nowInSeconds + time), async)
        } else {
          if (async)
            mod ! ModDelete(el, async)
          else
            mod !? ModDelete(el, async)
        }
        true
      }
    ).getOrElse(false)
    
	def delta(key: String, mod: Long, async: Boolean): Long =
	  getUnexpired(key) match {
	    case None => -1L
	    case Some(el) => {
        val v = Math.max(0L, (try { new String(el.data, "US-ASCII").toLong } catch { case _ => 0L }) + mod)
        val s = v.toString
        set(el.updateData(s.getBytes), async) // TODO: Should use CAS here.
        v
      }
    }
    
  def append(el: MEntry, async: Boolean) =
    getUnexpired(el.key) match {
      case Some(elPrev) => set(elPrev.concat(el, elPrev), async) // TODO: Should use CAS here.
      case None => false
    }
  
  def prepend(el: MEntry, async: Boolean) =
    getUnexpired(el.key) match {
      case Some(elPrev) => set(el.concat(elPrev, elPrev), async) // TODO: Should use CAS here.
      case None => false
    }

  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean) =
    getUnexpired(el.key) match {
      case Some(elPrev) => 
        if (elPrev.cid == cidPrev) { // TODO: Need to move this into mod actor?
          set(el, async)
          "STORED"                   // TODO: Get rid of transport/protocol-ism here.
        } else
          "EXISTS"
      case None => "NOT_FOUND"
    }

	def keys = data.keys
	
	def flushAll(expTime: Long) {
	  for ((key, el) <- data)
      if (expTime == 0L) 
        mod ! ModDelete(el, true)
      else
        delete(key, expTime, true)
	}
	
	def stats: MServerStats = 
	  (mod !? MServerStatsRequest).asInstanceOf[MServerStats]

  // --------------------------------------------
  
  // Only this actor is allowed to update the data_i root, 
  // which serializing modifications.  
  //
  // Also, this actor manages the LRU list and evicts items when necessary.
  //
  // TODO: Should the mod actor be on its own separate real thread?
  // TODO: Need to flush LRU when memory gets tight.
  //
  private val mod = actor {
    val lruHead: LRUList = new LRUList(" head ", null, null) // Least recently used sentinel.
    val lruTail: LRUList = new LRUList(" tail ", null, null) // Most recently used sentinel.
    
    lruHead.append(lruTail)
    
    def touch(el: MEntry) =
      if (el.lru != null) {
          el.lru.remove
          lruTail.insert(el.lru)
      }
      
    var usedMemory = 0L // In bytes.
    var evictions  = 0L // Number of valid entries that were evicted.
    
    def evictCheck: Unit =
      if (limitMemory > 0L &&
          limitMemory <= usedMemory)    // TODO: Need some slop when doing eviction check?
        evict(usedMemory - limitMemory) // TODO: Should evict slightly more as padding?
      
    def evict(numBytes: Long): Unit = {
      val now       = nowInSeconds
      var dataMod   = data         // Snapshot data outside of loop.
      var reclaimed = 0L           // Total number of bytes reclaimed in this call.
      var current   = lruHead.next // Start from least recently used.
      
      while (reclaimed < numBytes &&
             current != lruTail &&
             current != null) {
        val n = current.next
        current.remove
        dataMod.get(current.key).foreach(
          existing => {
            dataMod   = dataMod - current.key
            reclaimed = reclaimed + existing.dataSize

            if (!existing.isExpired(now))
              evictions = evictions + 1
          }
        )
        current = n
      }
      
      data_i_!!(dataMod)
      usedMemory = usedMemory - reclaimed
    }
    
    loop {
      react {
        case ModSet(el, noReply) => {
          val dataMod = data

          if (el.lru == null) {
              dataMod.get(el.key).foreach(
                existing => { 
                  el.lru = existing.lru 
                  
                  // Keep stats correct on update/set to existing key.
                  //
                  usedMemory = usedMemory - existing.dataSize
                }
              )

              if (el.lru == null)
                  el.lru = new LRUList(el.key, null, null)
          }
          
          touch(el)

          data_i_!!(dataMod + (el.key -> el))
          usedMemory = usedMemory + el.dataSize
        
          if (!noReply) 
              reply(true)

          evictCheck
        }

        case ModDelete(el, noReply) => {
          val dataMod = data
          
          dataMod.get(el.key) match {
            case Some(current) =>
              if (current.cid == el.cid) {
                if (el.lru != null) 
                    el.lru.remove
                
                data_i_!!(dataMod - el.key)
                usedMemory = usedMemory - el.dataSize
                
                if (!noReply) 
                    reply(true)
              } else {
                if (!noReply) 
                    reply(false)
              }
              
            case None =>
              if (!noReply) 
                  reply(false)
          }
        }
        
        case ModTouch(els, noReply) => {
          for (el <- els) {
            if (el.lru != null &&
                el.lru.next != null && // The entry might have been deleted already
                el.lru.prev != null)   // so don't put it back.
              touch(el)
          }
          
          if (!noReply) 
              reply(true)
        }
        
        case MServerStatsRequest() =>
          reply(MServerStats(data.size, usedMemory, evictions))
      }
    }
  }
  
  mod.start
  
  case class ModSet    (el: MEntry, noReply: Boolean)
  case class ModDelete (el: MEntry, noReply: Boolean)
  case class ModTouch  (els: Seq[MEntry], noReply: Boolean)
}

case class MServerStatsRequest
case class MServerStats(numEntries: Long,
                        usedMemory: Long,
                        evictions: Long) {
  def +(that: MServerStats) =
    MServerStats(numEntries + that.numEntries, 
                 usedMemory + that.usedMemory, 
                 evictions  + that.evictions)
}

// -------------------------------------------------------

case class MEntry(key: String, 
                  flags: Long,
                  expTime: Long,     // Expiry timestamp, in seconds since epoch.
                  dataSize: Int, 
                  data: Array[Byte],
                  cid: Long) {       // Unique id for CAS operations.
  def isExpired: Boolean = 
      isExpired(nowInSeconds)
  
  def isExpired(now: Long): Boolean = 
    expTime != 0L &&
    expTime < now

  // TODO: Revisit the cid + 1L update, as concurrent threads could
  //       generate the same updated cid number.  Not sure if that's
  //       a problem.
  // 
  def updateExpTime(e: Long) =
    MEntry(key, flags, e, dataSize, data, cid + 1L)

  def updateData(d: Array[Byte]) =
    MEntry(key, flags, expTime, d.length, d, cid + 1L)
  
  /**
   * Concatenate the data arrays from this with that,
   * using the basis for all other fields.
   */
  def concat(that: MEntry, basis: MEntry) = {
    val sizeNew = this.dataSize + that.dataSize
    val dataNew = new Array[Byte](sizeNew)

    System.arraycopy(this.data, 0, dataNew, 0,             this.dataSize)
    System.arraycopy(that.data, 0, dataNew, this.dataSize, that.dataSize)    
    
    MEntry(basis.key, 
           basis.flags, 
           basis.expTime,
           sizeNew,
           dataNew,
           basis.cid + 1L)
  }

  /**
   * Only the MSubServer mod actor should read or use the lru field.
   * Note that we don't copy the lru field during updateXxx() or concat()
   * so that the mod actor can do proper stats accounting.
   */  
  var lru: LRUList = null
}

// -------------------------------------------------------

class LRUList(var elem: String,
              var next: LRUList,
              var prev: LRUList) 
  extends mutable.DoubleLinkedList[String, LRUList] {
  def key = elem
}

