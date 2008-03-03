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
class MServer(subServerNum: Int) {
  def this() = this(Runtime.getRuntime.availableProcessors)
  
  private val subServers = new Array[MSubServer](subServerNum)
  for (i <- 0 until subServerNum)
    subServers(i) = createSubServer
    
  def createSubServer: MSubServer = new MSubServer // For overridability.
  
  def subServerForKey(key: String) = 
    if (subServerNum <= 1)
        subServers(0)
    else
        subServers(key.hashCode % subServerNum)

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
    
	def keys = subServers.foldLeft(List[String]().elements)(
	            (accum, next) => next.keys.append(accum))
	
	def flushAll(expTime: Long) = subServers.foreach(_.flushAll(expTime))
}

// --------------------------------------------

class MSubServer {
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
    data.get(key) match {
      case s @ Some(el) => if (el.isExpired) None else s
      case None => None
    }

  def get(key: String): Option[MEntry] =
    getUnexpired(key) match {
      case x @ Some(el) => mod ! ModTouch(el, true); x
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

  // --------------------------------------------
  
  // Only this actor is allowed to update the data_i root, 
  // which serializing writes.  
  //
  // Also, this actor manages the LRU list.
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
    
    loop {
      react {
        case ModSet(el, noReply) => 
          if (el.lru == null) {
              data.get(el.key).foreach(existing => { el.lru = existing.lru })               
              if (el.lru == null)
                  el.lru = new LRUList(el.key, null, null)
          }
          
          touch(el)
          data_i_!!(data + (el.key -> el))
          if (!noReply) 
              reply(true)
        
        case ModDelete(el, noReply) => 
          if (el.lru != null) 
              el.lru.remove
          data_i_!!(data - el.key)
          if (!noReply) 
              reply(true)
        
        case ModTouch(el, noReply) => 
          touch(el)
          if (!noReply) 
              reply(true)
      }
    }
  }
  
  mod.start
  
  case class ModSet    (el: MEntry, noReply: Boolean)
  case class ModDelete (el: MEntry, noReply: Boolean)
  case class ModTouch  (el: MEntry, noReply: Boolean)
}

// -------------------------------------------------------

case class MEntry(key: String, 
                  flags: Long,
                  expTime: Long,     // Expiry timestamp, in seconds since epoch.
                  dataSize: Int, 
                  data: Array[Byte],
                  cid: Long) {       // Unique id for CAS operations.
  def isExpired = expTime != 0L &&
                  expTime < nowInSeconds
  
  // TODO: Revisit the cid + 1L update, as concurrent threads could
  //       generate the same updated cid number.  Not sure if that's
  //       a problem.
  // 
  def updateExpTime(e: Long) =
    MEntry(key, flags, e, dataSize, data, cid + 1L).lru_!(lru)

  def updateData(d: Array[Byte]) =
    MEntry(key, flags, expTime, d.length, d, cid + 1L).lru_!(lru)
  
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
           basis.cid + 1L).lru_!(basis.lru)
  }
  
  var lru: LRUList = null
  def lru_!(x: LRUList) = { lru = x; this }
}

// -------------------------------------------------------

class LRUList(var elem: String,
              var next: LRUList,
              var prev: LRUList) 
  extends mutable.DoubleLinkedList[String, LRUList] {
  def key = elem
}

