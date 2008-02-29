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
  // TODO: Need to add LRU capability, see apache.commons.LRUMap.
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

  def get(key: String): Option[MEntry] = getUnexpired(key)
	
  def set(el: MEntry) = {
    mod ! ModSet(el) // TODO: Async semantics might be unexpected.
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

  def delete(key: String, time: Long) = 
		getUnexpired(key).map(
		  el => {
		    if (time != 0L) {
          if (el.expTime == 0L || 
              el.expTime > (nowInSeconds + time)) 
              set(el.updateExpTime(nowInSeconds + time))
        } else 
          mod ! ModDelete(key) // TODO: Async semantics might be unexpected.
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
        if (elPrev.cid == cidPrev) {
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
        mod ! ModDelete(key)
      else
        delete(key, expTime)
	}

  // --------------------------------------------
  
  // Only this actor is allowed to update the data, 
  // effectively serializing writes.
  //
  // TODO: Should the mod actor be on its own separate real thread?
  //
  private val mod = actor {
    loop {
      react {
        case ModSet(el)     => data_i_!!(data + (el.key -> el))
        case ModDelete(key) => data_i_!!(data - key)
      }
    }
  }
  
  mod.start
  
  case class ModSet(el: MEntry)
  case class ModDelete(key: String)
}

// -------------------------------------------------------

case class MEntry(key: String, 
                  flags: Long,
                  expTime: Long,     // Expiry timestamp, in seconds since epoch.
                  dataSize: Int, 
                  data: Array[Byte],
                  cid: Long) {       // Unique id for CAS operations.
  def isExpired = expTime < nowInSeconds
  
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
}

// -------------------------------------------------------

class LRUList(var elem: String,
              var next: LRUList,
              var prev: LRUList) 
  extends mutable.DoubleLinkedList[String, LRUList] {
  def key = elem
}
