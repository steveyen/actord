package ff.actord

import scala.collection._
import scala.actors._
import scala.actors.Actor._

import ff.actord.Util._

/**
 * Tracks key/value entries.
 *
 * We use immutable tree structures for better multi-core read/get performance, 
 * and for poor man's MVCC.
 *
 * Callers can pass in storage.SMap instead of TreeMap for data persistence.
 *
 * We ideally want this class to be as clear as possible of any 
 * dependencies on transport or wire protocol.
 */
class MServer(dataStart: immutable.SortedMap[String, MEntry]) {
  // TODO: Maybe just use volatile, or AtomicReference around data_i.
  //
  private var data_i = dataStart
  
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
	
  def set(el: MEntry): Boolean = 
      set(el, false)
	
  def set(el: MEntry, async: Boolean): Boolean = {
    if (async)
      mod ! ModSet(el, async)
    else
      mod !? ModSet(el, async)
    true
	}

  def add(el: MEntry) = 
    getUnexpired(el.key) match {
      case Some(_) => false
      case None => set(el)
    }

  def replace(el: MEntry) = 
    getUnexpired(el.key) match {
      case Some(_) => set(el)
      case None => false
    }

  def delete(key: String, time: Long): Boolean = 
      delete(key, time, false)

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
    
	def delta(key: String, mod: Long) =
	  getUnexpired(key) match {
	    case None => "NOT_FOUND"
	    case Some(el) => {
        val v = Math.max(0L, (try { new String(el.data, "US-ASCII").toLong } catch { case _ => 0L }) + mod)
        val s = v.toString
        set(el.updateData(s.getBytes)) // TODO: Should use CAS here.
        s
      }
    }
    
  def append(el: MEntry) =
    getUnexpired(el.key) match {
      case Some(elPrev) => set(elPrev.concat(el, elPrev)) // TODO: Should use CAS here.
      case None => false
    }
  
  def prepend(el: MEntry) =
    getUnexpired(el.key) match {
      case Some(elPrev) => set(el.concat(elPrev, elPrev)) // TODO: Should use CAS here.
      case None => false
    }

  def checkAndSet(el: MEntry, cidPrev: Long) =
    getUnexpired(el.key) match {
      case Some(elPrev) => 
        if (elPrev.cid == cidPrev) { // TODO: Need to move this into mod actor?
          set(el)
          "STORED"
        } else
          "EXISTS"
      case None => "NOT_FOUND"
    }

	def keys = data.keys
	
	def flushAll(expTime: Long) {
	  for ((key, el) <- data)
      if (expTime == 0L) 
        mod ! ModDelete(el, false)
      else
        delete(key, expTime)
	}

  // --------------------------------------------
  
  // Only this actor is allowed to update the data_i root, 
  // which serializing writes.  
  //
  // Also, this actor manages the LRU list.
  //
  // TODO: Should the mod actor be on its own separate real thread?
  // TODO: Need to flush LRU when memory gets tight.
  // TODO: For multi-core write scalability, need to have more than
  //       one writer actor?  Possibly via more than one tree root,
  //       such as one tree root (or MServer) per CPU processor thread.
  //       In that world, transport protocol handlers have to dispatch
  //       to the right MServer.
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

