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

class MSubServer(val id: Int, val limitMemory: Long) {
  /**
   * Override to pass in other implementations, such as storage.SMap for persistence.
   */
  def createSortedMap: immutable.SortedMap[String, MEntry] =
                   new immutable.TreeMap[String, MEntry]

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
      getUnexpired(key, data, true)
    
  def getUnexpired(key: String, 
                   dataMap: immutable.SortedMap[String, MEntry],
                   queueExpired: Boolean): Option[MEntry] =
    dataMap.get(key) match {
      case s @ Some(el) => {
        if (el.isExpired) {
          if (queueExpired)
            mod ! ModDelete(el, 0L, true) // TODO: Many readers of expired entries means redundant ModDelete messages.
          None 
        } else 
          s
      }
      case None => None
    }

  def getMulti(keys: Seq[String]): Iterator[MEntry] = {
    // Grab the data snapshot just once, outside the loop.
    //
    val d = data     
    val r = keys.flatMap(key => getUnexpired(key, d, true))

    if (!r.isEmpty)
      mod ! ModTouch(r.elements, true)
      
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
          mod ! ModDelete(el, time, async)
        else
          mod !? ModDelete(el, time, async)
        true
      }
    ).getOrElse(false)
    
  def add(el: MEntry, async: Boolean) = 
    if (async) {
      mod ! ModAdd(el, true, async)
      true
    } else
      (mod !? ModAdd(el, true, async)).asInstanceOf[Boolean]

  def replace(el: MEntry, async: Boolean) = 
    if (async) {
      mod ! ModAdd(el, false, async)
      true
    } else
      (mod !? ModAdd(el, false, async)).asInstanceOf[Boolean]

  def delta(key: String, v: Long, async: Boolean): Long =
    if (async) {
      mod ! ModDelta(key, v, async)
      0L
    } else
      (mod !? ModDelta(key, v, async)).asInstanceOf[Long]
    
  def xpend(el: MEntry, append: Boolean, async: Boolean) =
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
      mod ! ModDelete(el, expTime, true)
  }
  
  def stats: MServerStats = 
    (mod !? MServerStatsRequest).asInstanceOf[MServerStats]

  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    var r = data.range(keyFrom, keyTo)
    mod ! ModTouch(r.values, true)
    r.values
  }
  
  // --------------------------------------------
  
  /**
   * Only the "mod" actor is allowed to update the data_i root. 
   * Also, the mod actor manages the LRU list and evicts entries when necessary.
   * This design serializes modifications, which is why we usually create one 
   * MSubServer (and a matching mod actor) per processor core, to increase
   * writer concurrency.
   *
   * TODO: Should the mod actor be on its own separate real, receive-based thread?
   */
  val mod = actor {
    val lruHead: LRUList = new LRUList(" head ", null, null) // Least recently used sentinel.
    val lruTail: LRUList = new LRUList(" tail ", null, null) // Most recently used sentinel.
    
    lruHead.append(lruTail)
    
    def touch(el: MEntry) =
      if (el.lru != null) {
          el.lru.remove
          lruTail.insert(el.lru)
      }
      
    var usedMemory = 0L // In bytes.
    var evictions  = 0L // Number of valid entries that were evicted since server start.

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

          if (el.lru == null)
              el.lru = new LRUList(el.key, null, null)
      }
      
      touch(el)

      data_i_!!(dataMod + (el.key -> el))
      usedMemory = usedMemory + el.dataSize
      
      true
    }
    
    loop {
      receive {
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
        
        case ModSet(el, noReply) => {
          setEntry(el)
          if (!noReply) 
              reply(true)
          evictCheck
        }

        case ModDelete(el, expTime, noReply) => {
          val dataMod = data

          dataMod.get(el.key) match {
            case Some(current) =>
              if (current.cid == el.cid) {
                if (expTime != 0L) {
                  if (el.expTime == 0L || 
                      el.expTime > (nowInSeconds + expTime)) 
                    setEntry(el.updateExpTime(nowInSeconds + expTime))
                  if (!noReply)
                    reply(true)
                } else {
                  if (el.lru != null) 
                      el.lru.remove
                  
                  data_i_!!(dataMod - el.key)
                  usedMemory = usedMemory - el.dataSize
                  
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

        case ModAdd(el, add, noReply) => {
          val result = (getUnexpired(el.key) match {
            case Some(_) => if (add) false else setEntry(el)
            case None    => if (add) setEntry(el) else false
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
          reply(MServerStats(data.size, usedMemory, evictions))
      }
    }
  }
  
  val modThread = new Thread() { override def run = mod.start }
  
  case class ModSet    (el: MEntry, noReply: Boolean)
  case class ModDelete (el: MEntry, expTime: Long,   noReply: Boolean)
  case class ModTouch  (els: Iterator[MEntry],       noReply: Boolean)
  case class ModDelta  (key: String, delta: Long,    noReply: Boolean)
  case class ModXPend  (el: MEntry, append: Boolean, noReply: Boolean) // For append/prepend.
  case class ModCAS    (el: MEntry, cidPrev: Long,   noReply: Boolean)
  case class ModAdd    (el: MEntry, add: Boolean,    noReply: Boolean) // For add/replace.
}

