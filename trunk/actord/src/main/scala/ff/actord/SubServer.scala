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

    lruTouchManyWithStats(r.elements, keys.length)
      
    r.elements
  }

  def set(el: MEntry, async: Boolean) =
    modify(ModSet(el, async), async, true)

  def delete(key: String, time: Long, async: Boolean) = 
    getUnexpired(key).map(el => modify(ModDelete(key, el, time, async), async, true)).
                      getOrElse(false)
    
  def delta(key: String, v: Long, async: Boolean): Long =
    modify(ModDelta(key, v, async), async, 0L)
    
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean) = // For add or replace.
    modify(ModAddRep(el, isAdd, async), async, true)

  def xpend(el: MEntry, append: Boolean, async: Boolean) = // For append or prepend.
    modify(ModXPend(el, append, async), async, true)

  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean) =
    modify(ModCAS(el, cidPrev, async), async, "N/A")

  def keys = data.keys
  
  def flushAll(expTime: Long): Unit =
    for ((key, el) <- data)
      mod ! ModDelete(key, el, expTime, true)
  
  def stats: MServerStats = 
    (mod !? MServerStatsRequest).asInstanceOf[MServerStats]

  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    var r = data.range(keyFrom, keyTo)
    lruTouchManyWithStats(r.values, 0)
    r.values
  }
  
  def act(el: MEntry, async: Boolean) = Iterator.empty  
  
  def subServerList = List(this) // The only subServer we know about is self.
  
  // --------------------------------------------

  protected var stats_cmd_gets   = 0L // This stat is 'covered' by the lruHead sync object.
  protected var stats_cmd_sets   = 0L // This stat is 'covered' by the mod actor sync loop.
  protected var stats_get_hits   = 0L // This stat is 'covered' by the lruHead sync object.
  protected var stats_get_misses = 0L // This stat is 'covered' by the lruHead sync object.

  // --------------------------------------------

  val lruHead: LRUList = new LRUList(" head ", null, null) // Least recently used sentinel.
  val lruTail: LRUList = new LRUList(" tail ", null, null) // Most recently used sentinel.

  lruHead.append(lruTail) // The lruHead is also used for synchronization.

  def lruTouchManyWithStats(els: Iterator[MEntry], numSought: Int): Unit = lruHead.synchronized {
    var numTouched = lruTouchMany(els)

    stats_get_hits += numTouched // TODO: How to account for range hits?
    if (numSought > 0)           // TODO: How does memcached count get-multi?
      stats_cmd_gets += 1L         
    if (numSought > numTouched)
      stats_get_misses += (numSought - numTouched)
  }

  def lruTouchMany(els: Iterator[MEntry]): Int = lruHead.synchronized {
    var n = 0
    for (el <- els) {
      if (el.lru != null &&
          el.lru.next != null && // The entry might have been deleted already
          el.lru.prev != null)   // so don't put it back.
        lruTouch(el)
      n += 1
    }
    n
  }
  
  def lruTouch(el: MEntry): Unit = lruHead.synchronized {
    if (el.lru != null) {
        el.lru.remove
        lruTail.insert(el.lru)
    }
  }

  def lruRemove(x: LRUList): Unit = 
    if (x != null)
      lruHead.synchronized { x.remove }

  // --------------------------------------------

  def modify[T](msg: Object, async: Boolean, valWhenAsync: T): T = 
    if (async) {
      mod ! msg
      valWhenAsync
    } else 
      (mod !? msg).asInstanceOf[T]

  /**
   * Only this mod (or modification) actor is allowed to update the data_i root. 
   * Also, the mod actor works with the LRU list and evicts entries when necessary.
   * This design serializes modifications, which is why we usually create one 
   * MSubServer (and a matching mod actor) per processor core, to increase
   * writer concurrency.
   *
   * Note: The receive-based loop is mysteriously much faster than a react-based loop.
   */
  val mod = actor {
    // TODO: When using persistence, these stats are not updated as
    //       data is transparently swizzled in from storage.
    //
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

      lruHead.synchronized {
        var current = lruHead.next // Start from least recently used.
        
        while (reclaimed < numBytes &&
               current != lruTail &&
               current != null) {
          val n = current.next
          current.remove
          dataMod.get(current.key).foreach(
            existing => {
              dataMod   = dataMod - current.key
              reclaimed = reclaimed + existing.data.size
  
              if (!existing.isExpired(now))
                evictions += 1L
            }
          )
          current = n
        }
      }
      
      data_i_!!(dataMod)
      usedMemory -= reclaimed
    }
    
    def setEntry(el: MEntry): Boolean = {
      val dataMod = data

      if (el.lru == null) {
          dataMod.get(el.key).foreach(
            existing => { 
              el.lru = existing.lru 
              
              // Keep stats correct on update/set to existing key.
              //
              usedMemory -= existing.data.size
            }
          )

          if (el.lru == null)
              el.lru = new LRUList(el.key, null, null)
      }
      
      lruTouch(el)

      data_i_!!(dataMod + (el.key -> el))
      usedMemory += el.data.size
      
      true
    }
    
    loop {
      receive {
        case ModSet(el, noReply) => {
          setEntry(el)
          if (!noReply) 
              reply(true)
          evictCheck
          stats_cmd_sets += 1L
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
                  lruRemove(el.lru)
                  
                  data_i_!!(dataMod - key)
                  usedMemory -= el.data.size
                } else {
                  if (el.expTime == 0L || 
                      el.expTime > (nowInSeconds + expTime)) 
                    setEntry(el.updateExpTime(nowInSeconds + expTime))
                }
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
              val v = Math.max(0L, (try { arrayToString(el.data).toLong } catch { case _ => 0L }) + delta)
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
                             stats_cmd_gets, stats_cmd_sets,
                             stats_get_hits, stats_get_misses))
      }
    }
  }
  
  val modThread = new Thread() { override def run = mod.start }
  
  case class ModSet    (el: MEntry, noReply: Boolean)
  case class ModDelete (key: String, el: MEntry, expTime: Long, noReply: Boolean)
  case class ModDelta  (key: String, delta: Long,    noReply: Boolean)
  case class ModAddRep (el: MEntry, isAdd: Boolean,  noReply: Boolean) // For add/replace.
  case class ModXPend  (el: MEntry, append: Boolean, noReply: Boolean) // For append/prepend.
  case class ModCAS    (el: MEntry, cidPrev: Long,   noReply: Boolean)
}

