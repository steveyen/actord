package ff.actord

import ff.actord.Util._

/**
 * Tracks key/value entries.
 *
 * We want MServer to clear of any dependencies on wire protocol.
 */
class MServer {
  // TODO: Use storage.SMap for data persistence and query capability.
  // TODO: Need to add LRU capability, see apache.commons.LRUMap.
  //
  val data = new scala.collection.mutable.HashMap[String, MEntry] with
                 scala.collection.mutable.SynchronizedMap[String, MEntry]
  
  def getUnexpired(key: String): Option[MEntry] =
    data.get(key) match {
      case s @ Some(el) => if (el.isExpired) None else s
      case None => None
    }

  def get(key: String): Option[MEntry] =
    getUnexpired(key)
	
  def set(el: MEntry) = {
    data + (el.key -> el)
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
              data + (el.key -> el.updateExpTime(nowInSeconds + time))
    		} else 
    		  data - key
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
        data + (el.key -> el.updateData(s.getBytes))
        s
      }
    }
  
	def keys = data.keys
	
	def flushAll(expTime: Long) {
	  for ((key, el) <- data)
	    if (expTime == 0L)
	      data - key
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

