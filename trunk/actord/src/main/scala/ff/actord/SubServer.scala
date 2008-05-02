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

class MSubServer(val id: Int, val limitMemory: Long)
  extends MServer {
  /**
   * Override to pass in other implementations, such as for persistence.
   */
  def createSortedMap: immutable.SortedMap[String, MEntry] =
                   new ff.collection.Treap[String, MEntry]

  /**
   * TODO: Maybe just use volatile, or AtomicReference around data_i.
   */
  protected var data_i = createSortedMap
  
  protected def data_i_!!(d: immutable.SortedMap[String, MEntry]) = 
    synchronized { data_i = d }  
  
  /**
   * The data method allows threads to snapshot the immutable tree's root,
   * which is great for concurrent readers, who can then 
   * concurrently walk the tree.
   */
  def data = synchronized { data_i } 
  
  // --------------------------------------------

  def getUnexpired(key: String): Option[MEntry] =
      getUnexpired(key, data)
    
  def getUnexpired(key: String, 
                   map: immutable.SortedMap[String, MEntry]): Option[MEntry] =
    map.get(key) match {
      case s @ Some(el) => {
        if (el.isExpired) {
          mod ! ModDelete(key, el, 0L, true) // TODO: Many readers of expired entries means redundant ModDelete messages.
          None 
        } else 
          s
      }
      case _ => None
    }

  def get(keys: Seq[String]): Iterator[MEntry] = {
    // Grab the data snapshot just once, outside the loop.
    //
    val d = data     
    val r = keys.flatMap(key => getUnexpired(key, d))

    mod ! ModTouch(r.elements, keys.length)
      
    r.elements
  }

  def set(el: MEntry, async: Boolean) = {
    if (async)
      mod ! ModSet(el, async)
    else
      mod !? ModSet(el, async)
    true
  }

  def delete(key: String, time: Long, async: Boolean) = 
    getUnexpired(key).map(
      el => {
        if (async)
          mod ! ModDelete(key, el, time, async)
        else
          mod !? ModDelete(key, el, time, async)
        true
      }
    ).getOrElse(false)
    
  def delta(key: String, v: Long, async: Boolean): Long =
    if (async) {
      mod ! ModDelta(key, v, async)
      0L
    } else
      (mod !? ModDelta(key, v, async)).asInstanceOf[Long]
    
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean) = // For add or replace.
    if (async) {
      mod ! ModAddRep(el, isAdd, async)
      true
    } else
      (mod !? ModAddRep(el, isAdd, async)).asInstanceOf[Boolean]

  def xpend(el: MEntry, append: Boolean, async: Boolean) = // For append or prepend.
    if (async) {
      mod ! ModXPend(el, append, async)
      true
    } else
      (mod !? ModXPend(el, append, async)).asInstanceOf[Boolean]

  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean) =
    if (async) {
      mod ! ModCAS(el, cidPrev, async)
      "N/A"
    } else
      (mod !? ModCAS(el, cidPrev, async)).asInstanceOf[String]

  def keys = data.keys
  
  def flushAll(expTime: Long) {
    for ((key, el) <- data)
      mod ! ModDelete(key, el, expTime, true)
  }
  
  def stats: MServerStats = 
    (mod !? MServerStatsRequest).asInstanceOf[MServerStats]

  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    var r = data.range(keyFrom, keyTo)
    mod ! ModTouch(r.values, 0)
    r.values
  }
  
  def act(el: MEntry, async: Boolean) = Iterator.empty  
  
  def subServerList = List(this) // The only subServer we know about is self.
  
  // --------------------------------------------
  
  /**
   * Only this mod (or modification) actor is allowed to update the data_i root. 
   * Also, the mod actor manages the LRU list and evicts entries when necessary.
   * This design serializes modifications, which is why we usually create one 
   * MSubServer (and a matching mod actor) per processor core, to increase
   * writer concurrency.
   *
   * Note: The receive-based loop is mysteriously much faster than a react-based loop.
   */
  val mod = actor {
    val lruHead: LRUList = new LRUList(" head ", null, null) // Least recently used sentinel.
    val lruTail: LRUList = new LRUList(" tail ", null, null) // Most recently used sentinel.
    var lruSize = 0L

    lruHead.append(lruTail)
    
    def touch(el: MEntry) =
      if (el.lru != null) {
          el.lru.remove
          lruTail.insert(el.lru)
      }

    // TODO: When using persistence, these stats are not updated as
    //       data is transparently swizzled in from storage.
    //
    var usedMemory = 0L // In bytes.
    var evictions  = 0L // Number of valid entries that were evicted since server start.
    var cmd_gets   = 0L
    var cmd_sets   = 0L
    var get_hits   = 0L
    var get_misses = 0L

    /**
     * Runs eviction if memory is over limits.
     */    
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
        lruSize = lruSize - 1L
        dataMod.get(current.key).foreach(
          existing => {
            dataMod   = dataMod - current.key
            reclaimed = reclaimed + existing.dataSize

            if (!existing.isExpired(now))
              evictions = evictions + 1L
          }
        )
        current = n
      }
      
      data_i_!!(dataMod)
      usedMemory = usedMemory - reclaimed
    }
    
    def setEntry(el: MEntry): Boolean = {
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

          if (el.lru == null) {
              el.lru = new LRUList(el.key, null, null)
              lruSize = lruSize + 1L
          }
      }
      
      touch(el)

      data_i_!!(dataMod + (el.key -> el))
      usedMemory = usedMemory + el.dataSize
      
      true
    }
    
    loop {
      receive {
        case ModTouch(els, numGetMultiKeys) => {
          var numHits = 0
          for (el <- els) {
            if (el.lru != null &&
                el.lru.next != null && // The entry might have been deleted already
                el.lru.prev != null)   // so don't put it back.
              touch(el)
            numHits = numHits + 1
          }
          
          get_hits = get_hits + numHits // TODO: How to account for range hits?

          if (numGetMultiKeys > 0)
            cmd_gets = cmd_gets + 1L    // TODO: How does memcached count get-multi?
          
          if (numGetMultiKeys > numHits)
            get_misses = get_misses + (numGetMultiKeys - numHits)
        }
        
        case ModSet(el, noReply) => {
          setEntry(el)
          if (!noReply) 
              reply(true)
          evictCheck
          cmd_sets = cmd_sets + 1L
        }

        case ModDelete(key, el, expTime, noReply) => {
          val dataMod = data

          dataMod.get(key) match {
            case Some(current) =>
              val isErrorEntry = el.key == null
              if (isErrorEntry ||          // Always delete error entries.
                  current.cid == el.cid) { // Or, must have the same CAS value.
                if (isErrorEntry ||
                    expTime == 0L) {
                  if (el.lru != null) {
                      el.lru.remove
                      lruSize = lruSize - 1L
                  }
                  
                  data_i_!!(dataMod - key)
                  usedMemory = usedMemory - el.dataSize
                  
                  if (!noReply) 
                      reply(true)
                } else {
                  if (el.expTime == 0L || 
                      el.expTime > (nowInSeconds + expTime)) 
                    setEntry(el.updateExpTime(nowInSeconds + expTime))
                  if (!noReply)
                    reply(true)
                }
              } else {
                if (!noReply) 
                    reply(false)
              }
              
            case None =>
              if (!noReply) 
                  reply(false)
          }
        }

        case ModAddRep(el, isAdd, noReply) => {
          val result = (getUnexpired(el.key) match {
            case Some(_) => if (isAdd) false else setEntry(el)
            case None    => if (isAdd) setEntry(el) else false
          })
          if (!noReply) 
              reply(result)
        }

        case ModDelta(key, delta, noReply) => {
          val result = (getUnexpired(key) match {
            case None => -1L
            case Some(el) => {
              val v = Math.max(0L, (try { new String(el.data, "US-ASCII").toLong } catch { case _ => 0L }) + delta)
              val s = v.toString
              setEntry(el.updateData(s.getBytes))
              v
            }
          })
          if (!noReply)
            reply(result)
        }
    
        case ModXPend(el, append, noReply) => {
          val result = (getUnexpired(el.key) match {
            case Some(elPrev) => 
              if (append)
                setEntry(elPrev.concat(el, elPrev))
              else
                setEntry(el.concat(elPrev, elPrev))
            case None => false
          })
          if (!noReply)
            reply(result)
        }
        
        case ModCAS(el, cidPrev, noReply) => {
          val result = (getUnexpired(el.key) match {
            case Some(elPrev) => 
              if (elPrev.cid == cidPrev) {
                setEntry(el)
                "STORED"                   
              } else
                "EXISTS"
            case None => "NOT_FOUND" // TODO: Get rid of transport/protocol-isms.
          })
          if (!noReply)
            reply(result)
        }
        
        case MServerStatsRequest =>
          // TODO: The data.size method is slow / walks all nodes!
          //
          reply(MServerStats(data.size, usedMemory, evictions, 
                             cmd_gets, cmd_sets,
                             get_hits, get_misses,
                             lruSize))
      }
    }
  }
  
  val modThread = new Thread() { override def run = mod.start }
  
  case class ModSet    (el: MEntry, noReply: Boolean)
  case class ModDelete (key: String, el: MEntry, expTime: Long, noReply: Boolean)
  case class ModTouch  (els: Iterator[MEntry], numKeys: Int)
  case class ModDelta  (key: String, delta: Long,    noReply: Boolean)
  case class ModAddRep (el: MEntry, isAdd: Boolean,  noReply: Boolean) // For add/replace.
  case class ModXPend  (el: MEntry, append: Boolean, noReply: Boolean) // For append/prepend.
  case class ModCAS    (el: MEntry, cidPrev: Long,   noReply: Boolean)
}

