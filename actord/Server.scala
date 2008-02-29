package ff.actord

import scala.collection._

import ff.actord.Util._

/**
 * Tracks key/value entries.
 *
 * We want MServer to clear of any dependencies on wire protocol.
 *
 * Callers can pass in storage.SMap instead of TreeMap for data persistence.
 */
class MServer(dataStart: immutable.SortedMap[String, MEntry]) {
  // TODO: Need to add LRU capability, see apache.commons.LRUMap.
  // TODO: Maybe just use volatile, or AtomicReference here.
  //
  private var data_i = dataStart
  
  private def data_i_!!(d: immutable.SortedMap[String, MEntry]) = synchronized {
    data_i = d
  }  
  
  def data = synchronized { data_i } // Allows threads to snapshot the tree's root.

  def getUnexpired(key: String): Option[MEntry] =
    data.get(key) match {
      case s @ Some(el) => if (el.isExpired) None else s
      case None => None
    }

  def get(key: String): Option[MEntry] = getUnexpired(key)
	
  def set(el: MEntry) = synchronized {
    data_i_!!(data_i + (el.key -> el))
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
              data_i_!!(data + (el.key -> el.updateExpTime(nowInSeconds + time)))
    		} else 
    		  data_i_!!(data - key)
        true
      }
    ).getOrElse(false)
    
	def delta(key: String, mod: Long) =
	  getUnexpired(key) match {
	    case None => "NOT_FOUND"
	    case Some(el) => {
	      // TODO: Is US_ASCII right here?
	      //
        val v = Math.max(0, (try { new String(el.data, "US_ASCII").toLong } catch { case _ => 0L }) + mod)
        val s = v.toString
        data_i_!!(data + (el.key -> el.updateData(s.getBytes)))
        s
      }
    }
  
	def keys = data.keys
	
	def flushAll(expTime: Long) {
    for ((key, el) <- data)
  	  if (expTime == 0L) 
        data_i_!!(data - key)
      else
	      delete(key, expTime)
	}
}

// -------------------------------------------------------

case class MEntry(key: String, 
                  flags: Long, 
                  expTime: Long, 
                  dataSize: Int, 
                  data: Array[Byte]) {
  def isExpired = expTime < nowInSeconds
  
  def updateExpTime(e: Long) =
    MEntry(key, flags, e, dataSize, data)

  def updateData(d: Array[Byte]) =
    MEntry(key, flags, expTime, d.length, d)    
    
  def cas = "CAS_TODO" // TODO
}

