package gg.scalabase

import java.net._
import java.nio.charset._
import java.util.concurrent._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

object Main
{
  def main(args: Array[String]) {
    startAcceptor(Runtime.getRuntime.availableProcessors, 11211)
    println("listening on port " + 11211)
  }
  
  def startAcceptor(numProcessors: Int, port: Int) = 
    initAcceptor(new NioSocketAcceptor(numProcessors)).bind(new InetSocketAddress(port))
  
  def initAcceptor(acceptor: IoAcceptor) = {
    val codecFactory = new DemuxingProtocolCodecFactory

    codecFactory.addMessageDecoder(new MDecoder)
    codecFactory.addMessageEncoder(classOf[List[MResponse]], new MEncoder)
    
    acceptor.getFilterChain.
             addLast("codec", new ProtocolCodecFilter(codecFactory))  
    acceptor.setHandler(new MServer)
    acceptor
  }
}

// -------------------------------------------------------

object Util {
  val ZERO      = java.lang.Integer.valueOf(0)
  val CR        = '\r'.asInstanceOf[Byte]
  val CRNL      = "\r\n"
  val CRNLBytes = CRNL.getBytes
  val US_ASCII  = "US_ASCII"
  def nowInSeconds: Long = System.currentTimeMillis / 1000
}

import Util._

// -------------------------------------------------------

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
    "STORED"
	}
	
  def add(el: MEntry) = 
    getUnexpired(el.key) match {
      case Some(_) => "NOT_STORED"
      case None => set(el)
    }

  def replace(el: MEntry) = 
    getUnexpired(el.key) match {
      case Some(_) => set(el)
      case None => "NOT_STORED"
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
        "DELETED"
      }
    ).getOrElse("NOT_FOUND")
    
	def delta(key: String, mod: Long) =
	  getUnexpired(key) match {
	    case None => {
	      get_misses += 1
        "NOT_FOUND"
	    }
	    case Some(el) => {
        val v = Math.max(0, (try { new String(el.data, US_ASCII).toLong } catch { case _ => 0L }) + mod)
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
	
	def flushAll(expTime: Long): String = {
	  for ((key, el) <- data)
	    if (expTime == 0L)
	      data - key
	    else
	      delete(key, expTime)
	  "OK"
	}
}

// -------------------------------------------------------

/**
 * Represents a specification, of a command in the protocol.
 *
 * TODO: Need a better process() signature if we want to handle async streaming?
 *       Research how mina allows request and response streaming.
 */
case class Spec(line: String,
                process: (MServer, MCommand, IoSession) => List[MResponse]) {
  val args = line.split(" ")
  val name = args(0)
}

// -------------------------------------------------------

/**
 * Protocol is defined at:
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
 */
class MDecoder extends MessageDecoder {
  val charsetDecoder     = Charset.forName("UTF-8").newDecoder
  val SECONDS_IN_30_DAYS = 60*60*24*30
  val MIN_CMD_SIZE       = "quit \r\n".length
  val WAITING_FOR        = new AttributeKey(getClass, "waiting_for")  
  
  def decodable(session: IoSession, in: IoBuffer): MessageDecoderResult = {
    val waitingFor = session.getAttribute(WAITING_FOR, ZERO).asInstanceOf[java.lang.Integer].intValue
  	if (waitingFor == 0) {
  	  if (in.remaining >= MIN_CMD_SIZE)
  	    MessageDecoderResult.OK
  	  else
  	    MessageDecoderResult.NEED_DATA
  	}
  	
  	if (waitingFor <= in.remaining)
  	  MessageDecoderResult.OK
  	else	
    	MessageDecoderResult.NEED_DATA
  }
  
  /**
   * Commands defined with a single line.
   */
  val lineOnlySpecs = List( 
      Spec("get <key>*",
           (svr, cmd, sess) => {
             (1 until cmd.args.length).flatMap(
               i => svr.get(cmd.args(i)).
                        map(el => MResponseEntry(el.asValueLine, el, null)).
                        toList
             ).toList ::: MResponseLine("END") :: Nil
           }),
//    Spec("gets <key>*",
//         (svr, cmd, sess) => {
//           (1 until cmd.args.length).flatMap(
//             i => svr.get(cmd.args(i)).
//                      map(el => MResponseEntry(el.asValueLineCAS, el, null)).
//                      toList
//           ).toList ::: MResponseLine("END") :: Nil
//         }),
      Spec("delete <key> [<time>] [noreply]",
           (svr, cmd, sess) => {
             MResponseLine(svr.delete(cmd.args(1), cmd.argToLong(2))) :: Nil
           }),
      Spec("incr <key> <value> [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.delta(cmd.args(1), cmd.argToLong(2))) :: Nil),
      Spec("decr <key> <value> [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.delta(cmd.args(1), -1L * cmd.argToLong(2))) :: Nil),
      Spec("stats [<arg>]",
           (svr, cmd, sess) => MResponseLine(svr.stats(cmd.argOrElse(1, null))) :: Nil),
      Spec("flush_all [<delay>] [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.flushAll(cmd.argToLong(1))) :: Nil),
      Spec("version",
           (svr, cmd, sess) => MResponseLine("VERSION dunno") :: Nil),
      Spec("verbosity",
           (svr, cmd, sess) => MResponseLine("OK") :: Nil),
      Spec("quit",
           (svr, cmd, sess) => {
             sess.close
             Nil
           }))
           
  /**
   * Commands that use a line followed by byte data.
   */
  val lineWithDataSpecs = List( 
      Spec("set <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.set(cmd.entry)) :: Nil),
      Spec("add <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.add(cmd.entry)) :: Nil),
      Spec("replace <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => MResponseLine(svr.replace(cmd.entry)) :: Nil)
//    Spec("append <key> <flags> <expTime> <bytes> [noreply]",
//         (svr, cmd, sess) => MResponseLine(svr.append(cmd.entry)) :: Nil),
//    Spec("prepend <key> <flags> <expTime> <bytes> [noreply]",
//         (svr, cmd, sess) => MResponseLine(svr.prepend(cmd.entry)) :: Nil),
//    Spec("cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]",
//         ...)
      )
      
  val lineOnlyCommands = 
      Map[String, Spec](lineOnlySpecs.
                          map(s => Pair(s.name, s)):_*)

  val lineWithDataCommands = 
      Map[String, Spec](lineWithDataSpecs.
                          map(s => Pair(s.name, s)):_*)
                          
  // ----------------------------------------

  def decode(session: IoSession, in: IoBuffer, out: ProtocolDecoderOutput): MessageDecoderResult = {
  	val remaining = in.remaining
    val waitingFor = session.getAttribute(WAITING_FOR, ZERO).asInstanceOf[java.lang.Integer].intValue
  	if (waitingFor > 0) {
  	  if (waitingFor <= remaining)
        session.setAttribute(WAITING_FOR, ZERO)
      else
        return MessageDecoderResult.NEED_DATA
  	}
  	
  	val indexCR = in.indexOf(CR)
  	if (indexCR < 0)
  	    return MessageDecoderResult.NEED_DATA

  	if (indexCR + CRNL.length > remaining) 
  	    return MessageDecoderResult.NEED_DATA
  	    
    // TODO: At this point, indexCR + CRNL.length <= remaining,
    //       so we can handle one line.  But for single-line-only commands,
    //       does mina do the right thing with any bytes that
    //       come after indexCR + CRNL.length?
    //
    val line = in.getString(indexCR + CRNL.length, charsetDecoder)
    if (line.endsWith(CRNL) == false)
        return MessageDecoderResult.NOT_OK // TODO: Need to close session here?
        
    val args    = line.split(" ")
    val cmdName = args(0)

    lineOnlyCommands.get(cmdName).map(
      spec => {
        out.write(Pair(spec, MCommand(args, null)))
        MessageDecoderResult.OK
      }
    ) orElse lineWithDataCommands.get(cmdName).map(
      spec => {
  			// Handle mutator command: 
  			//   <cmdName> <key> <flags> <expTime> <bytes> [noreply]\r\n
        //   cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]\r\n
  			//
        if (args.length >= 5) {
          var dataSize  = args(4).toInt
          val totalSize = line.length + dataSize + CRNL.length
          if (totalSize <= remaining) {
            val expTime = args(3).toLong
            val data    = new Array[Byte](dataSize) // TODO: Handle this better when dataSize is huge.
  
            in.get(data)
            
            out.write(Pair(spec,
                           MCommand(args,
                                    MEntry(args(1),
                                           args(2).toLong,
                                           if (expTime != 0 &&
                                               expTime <= SECONDS_IN_30_DAYS)
                                               expTime + nowInSeconds
                                           else
                                               expTime,
                                           dataSize,
                                           data))))
            MessageDecoderResult.OK
          } else {
            session.setAttribute(WAITING_FOR, totalSize)
            in.rewind
            MessageDecoderResult.NEED_DATA
          }
        } else {
          out.write(MResponseLine("CLIENT_ERROR")) // Saw an ill-formed mutator command,
          MessageDecoderResult.OK                  // but keep going, and decode the next command.
        }
      }
    ) getOrElse {
      out.write(MResponseLine("ERROR")) // Saw an unknown command,
      MessageDecoderResult.OK           // but keep going, and decode the next command.
    }
  }
  
  def finishDecode(session: IoSession, out: ProtocolDecoderOutput) = {
    // TODO: Do we need to do something here?
  }
}

// -------------------------------------------------------

class MEncoder extends MessageEncoder {
	def encode(session: IoSession, message: Object, out: ProtocolEncoderOutput) {
    for (res <- message.asInstanceOf[List[MResponse]]) {
      val buf = IoBuffer.allocate(res.main.length + 
                                  CRNL.length +
                                  (if (res.entry != null) 
                                       res.entry.dataSize + CRNL.length + 
                                       res.suffix.length + CRNL.length
                                   else
                                       0))
      buf.setAutoExpand(true)
      buf.put(res.main.toString.getBytes)
      buf.put(CRNLBytes)
      
      if (res.entry != null) {
        buf.put(res.entry.data)
        buf.put(CRNLBytes)
        buf.put(res.suffix.toString.getBytes)
        buf.put(CRNLBytes)
      }
      
      buf.flip()
      out.write(buf)
      
      // TODO: Reuse the buf again as we loop.
    }
  }
}

// -------------------------------------------------------

case class MCommand(args: Array[String], entry: MEntry) {
  def noreply = args.last == "noreply"
  
  def argToLong(at: Int) =
    if (args.length > at) {
      try { args(at).toLong } catch { case _ => 0L }
    } else 
      0L
  
  def argOrElse(at: Int, defaultValue: String) = 
    if (args.length > at)
      args(at)
    else
      defaultValue
}

// -------------------------------------------------------

abstract class MResponse {
  def main: String
  def entry: MEntry
  def suffix: String
}

case class MResponseEntry(main: String,
                          entry: MEntry, 
                          suffix: String) extends MResponse

case class MResponseLine(main: String) extends MResponse {
  def entry = null
  def suffix = null
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
    
  def asValueLine =
      "VALUE " + key + " " + flags + " " + dataSize
  
  def asValueLineCAS =
      asValueLine + " " + cas
      
  def cas = "CAS_TODO" // TODO
}
