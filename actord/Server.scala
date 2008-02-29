package ff.actord

import java.net._
import java.nio.charset._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

class MServer extends IoHandlerAdapter {
  val log = LoggerFactory.getLogger(getClass)
  
  override def exceptionCaught(session: IoSession, cause: Throwable) = {
    log.warn("unexpected exception: ", cause)
    session.close
  }
  
  override def messageReceived(session: IoSession, message: Object): Unit = {
    log.info("received: " + message)
    
    message match {
      case (spec: Spec, cmd: MCommand) =>
        session.write(spec.process(this, cmd, session))
      case m: MResponse =>
        session.write(List(m))
    }
  }
  
  // ---------------------------------
  
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

  def get(key: String): Option[MEntry] = {
    get_cmds += 1
    getUnexpired(key) match {
	    case x @ None => {
	      get_misses += 1
        x
	    }
	    case x @ Some(el) => {
	      get_hits += 1
	      x
	    }
	  }
	}
	
  def set(el: MEntry) = {
    set_cmds += 1
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
	    case None => {
	      get_misses += 1
        "NOT_FOUND"
	    }
	    case Some(el) => {
	      // TODO: Is US_ASCII right here?
	      //
        val v = Math.max(0, (try { new String(el.data, "US_ASCII").toLong } catch { case _ => 0L }) + mod)
        val s = v.toString
        data + (el.key -> el.updateData(s.getBytes))
        s
      }
    }
    
	var set_cmds = 0
	var get_cmds = 0
	var get_hits = 0
	var get_misses = 0
	var curr_items = 0
	var total_items = 0
	
	def initStats = {
  	set_cmds = 0
  	get_cmds = 0
  	get_hits = 0
  	get_misses = 0
  	curr_items = 0
  	total_items = 0
	}
	
	def stats(arg: String) = {
	  var returnData = ""

		if (arg == "keys") {
		  for ((key, el) <- data)
		    returnData += ("STAT key " + key + CRNL)
		  returnData + "END"
		} else {
//      returnData += "STAT version " + version + CRNL

      returnData += "STAT cmd_gets " + String.valueOf(get_cmds)+ CRNL
      returnData += "STAT cmd_sets " + String.valueOf(set_cmds)+ CRNL
      returnData += "STAT get_hits " + String.valueOf(get_hits)+ CRNL
      returnData += "STAT get_misses " + String.valueOf(get_misses)+ CRNL

//      returnData += "STAT curr_connections " + String.valueOf(curr_conns)+ CRNL
//      returnData += "STAT total_connections " + String.valueOf(total_conns)+ CRNL
//      returnData += "STAT time " + String.valueOf(Now()) + CRNL
//      returnData += "STAT uptime " + String.valueOf(Now()-this.started) + CRNL
//      returnData += "STAT cur_items " + String.valueOf(this.data.size()) + CRNL
//      returnData += "STAT limit_maxbytes "+String.valueOf(maxbytes)+CRNL
//      returnData += "STAT current_bytes "+String.valueOf(Runtime.getRuntime().totalMemory())+CRNL
//      returnData += "STAT free_bytes "+String.valueOf(Runtime.getRuntime().freeMemory())+CRNL
      
      returnData += "STAT pid 0\r\n"
      returnData += "STAT rusage_user 0:0\r\n"
      returnData += "STAT rusage_system 0:0\r\n"
      returnData += "STAT connection_structures 0\r\n"
      returnData += "STAT bytes_read 0\r\n"
      returnData += "STAT bytes_written 0\r\n"
      returnData += "END\r\n"
      returnData
    }
	}
	
	def flushAll(expTime: Long) {
	  for ((key, el) <- data)
	    if (expTime == 0L)
	      data - key
	    else
	      delete(key, expTime)
	}
}

