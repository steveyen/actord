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

import java.net._
import java.nio.charset._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

class MHandler(server: MServer) extends IoHandlerAdapter {
  val log = LoggerFactory.getLogger(getClass)
  
  override def exceptionCaught(session: IoSession, cause: Throwable) = {
    log.warn("unexpected exception: ", cause)
    session.close
  }
  
  override def messageReceived(session: IoSession, message: Object): Unit = {
    log.info("received: " + message)
    
    message match {
      case (spec: Spec, cmd: MCommand) => {
        val res = spec.process(server, cmd, session)
        if (cmd.noReply == false)
          session.write(res)
      }
      case m: MResponse =>
        session.write(List(m))
    }
  }
}

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
 *
 * TODO: See if we can use something lower-level, like 
 *       CummulativeProtocolDecoder, for more performance.
 */
class MDecoder extends MessageDecoder {
  /**
   * Commands defined with a single line.
   */
  def lineOnlySpecs = List( 
      Spec("get <key>*",
           (svr, cmd, sess) => { 
             svr.getMulti(cmd.args.slice(1, cmd.args.length),
                          el => sess.write(List(MResponseLineEntry(asValueLine(el), el))))
             reply("END")
           }),

      Spec("gets <key>*",
           (svr, cmd, sess) => {
             svr.getMulti(cmd.args.slice(1, cmd.args.length),
                          el => sess.write(List(MResponseLineEntry(asValueLineCAS(el), el))))
             reply("END")
           }),

      Spec("delete <key> [<time>] [noreply]",
           (svr, cmd, sess) => 
             reply(svr.delete(cmd.args(1), cmd.argToLong(2), cmd.noReply), 
                   "DELETED", "NOT_FOUND")),

      Spec("incr <key> <value> [noreply]",
           (svr, cmd, sess) => reply(svr.delta(cmd.args(1),  cmd.argToLong(2), cmd.noReply))),
      Spec("decr <key> <value> [noreply]",
           (svr, cmd, sess) => reply(svr.delta(cmd.args(1), -cmd.argToLong(2), cmd.noReply))),
           
//    Spec("stats [<arg>]",
//         (svr, cmd, sess) => 
//           reply(svr.stats(cmd.argOrElse(1, null)))),

      Spec("flush_all [<delay>] [noreply]",
           (svr, cmd, sess) => {
              svr.flushAll(cmd.argToLong(1))
              reply("OK")
            }),

      Spec("version",
           (svr, cmd, sess) => reply("VERSION actord_0.0.0")),
      Spec("verbosity",
           (svr, cmd, sess) => reply("OK")),
      Spec("quit",
           (svr, cmd, sess) => {
             sess.close
             Nil
           }))
           
  /**
   * Commands that use a line followed by byte data.
   */
  def lineWithDataSpecs = List( 
      Spec("set <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.set(cmd.entry, cmd.noReply), 
                    "STORED", "NOT_STORED")),

      Spec("add <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.add(cmd.entry, cmd.noReply), 
                    "STORED", "NOT_STORED")),

      Spec("replace <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.replace(cmd.entry, cmd.noReply), 
                    "STORED", "NOT_STORED")),

      Spec("append <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.append(cmd.entry, cmd.noReply), 
                    "STORED", "NOT_STORED")),

      Spec("prepend <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.prepend(cmd.entry, cmd.noReply), 
                    "STORED", "NOT_STORED")),
           
      Spec("cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.checkAndSet(cmd.entry, cmd.argToLong(5), cmd.noReply))))
      
  val lineOnlyCommands = 
      Map[String, Spec](lineOnlySpecs.
                          map(s => Pair(s.name, s)):_*)

  val lineWithDataCommands = 
      Map[String, Spec](lineWithDataSpecs.
                          map(s => Pair(s.name, s)):_*)
                          
  // ----------------------------------------

  def asValueLine(e: MEntry) =
      "VALUE " + e.key + " " + e.flags + " " + e.dataSize
  
  def asValueLineCAS(e: MEntry) =
      asValueLine(e) + " " + e.cid
  
  // ----------------------------------------

  def reply(s: String): List[MResponse] = List(MResponseLine(s))
           
  def reply(v: Boolean, t: String, f: String): List[MResponse] =
      reply(if (v) t else f)
      
  def reply(v: Long): List[MResponse] =
      reply(if (v < 0)
              "NOT_FOUND"
            else
              v.toString)
                          
  // ----------------------------------------

  val charsetDecoder     = Charset.forName("UTF-8").newDecoder
  val SECONDS_IN_30_DAYS = 60*60*24*30
  val MIN_CMD_SIZE       = "quit \r\n".length
  val WAITING_FOR        = new AttributeKey(getClass, "waiting_for")  
  val STATS              = new AttributeKey(getClass, "stats")  
  
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
  	    
    val line = in.getString(indexCR + CRNL.length, charsetDecoder)
    if (line.endsWith(CRNL) == false)
        return MessageDecoderResult.NOT_OK // TODO: Need to close session here?
        
    val args    = line.trim.split(" ")
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
          var dataSize  = args(4).trim.toInt
          val totalSize = line.length + dataSize + CRNL.length
          if (totalSize <= remaining) {
            val expTime = args(3).trim.toLong
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
                                           data,
                                           (session.getId << 32) + 
                                           (session.getReadMessages & 0xFFFFFFFFL)))))
            MessageDecoderResult.OK
          } else {
            session.setAttribute(WAITING_FOR, new java.lang.Integer(totalSize))
            in.rewind
            MessageDecoderResult.NEED_DATA
          }
        } else {
          out.write(MResponseLine("CLIENT_ERROR " + cmdName)) // Saw an ill-formed mutator command,
          MessageDecoderResult.OK                             // but keep going, and decode the next command.
        }
      }
    ) getOrElse {
      out.write(MResponseLine("ERROR " + cmdName)) // Saw an unknown command,
      MessageDecoderResult.OK                      // but keep going, and decode the next command.
    }
  }
  
  def finishDecode(session: IoSession, out: ProtocolDecoderOutput) = {
    // TODO: Do we need to do something here?  Or just drop the message on the floor?
  }

  // ----------------------------------------

//	def stats(arg: String) = {
//	  var returnData = ""
//
//		if (arg == "keys") {
//		  for ((key, el) <- data)
//		    returnData += ("STAT key " + key + CRNL)
//		  returnData + "END"
//		} else {
//      returnData += "STAT version " + version + CRNL
//
//      returnData += "STAT cmd_gets " + String.valueOf(get_cmds)+ CRNL
//      returnData += "STAT cmd_sets " + String.valueOf(set_cmds)+ CRNL
//      returnData += "STAT get_hits " + String.valueOf(get_hits)+ CRNL
//      returnData += "STAT get_misses " + String.valueOf(get_misses)+ CRNL
//
//      returnData += "STAT curr_connections " + String.valueOf(curr_conns)+ CRNL
//      returnData += "STAT total_connections " + String.valueOf(total_conns)+ CRNL
//      returnData += "STAT time " + String.valueOf(Now()) + CRNL
//      returnData += "STAT uptime " + String.valueOf(Now()-this.started) + CRNL
//      returnData += "STAT cur_items " + String.valueOf(this.data.size()) + CRNL
//      returnData += "STAT limit_maxbytes "+String.valueOf(maxbytes)+CRNL
//      returnData += "STAT current_bytes "+String.valueOf(Runtime.getRuntime().totalMemory())+CRNL
//      returnData += "STAT free_bytes "+String.valueOf(Runtime.getRuntime().freeMemory())+CRNL
//      
//      returnData += "STAT pid 0\r\n"
//      returnData += "STAT rusage_user 0:0\r\n"
//      returnData += "STAT rusage_system 0:0\r\n"
//      returnData += "STAT connection_structures 0\r\n"
//      returnData += "STAT bytes_read 0\r\n"
//      returnData += "STAT bytes_written 0\r\n"
//      returnData += "END\r\n"
//      returnData
//    }
//	}
}

// -------------------------------------------------------

class MEncoder extends MessageEncoder {
	def encode(session: IoSession, message: Object, out: ProtocolEncoderOutput) {
	  val resList = message.asInstanceOf[List[MResponse]]
    val bufMax  = resList.foldLeft(0)((max, next) => Math.max(max, next.size))
    val buf     = IoBuffer.allocate(bufMax)

    buf.setAutoExpand(true)
    
    for (res <- resList) {
      res.put(buf)
      buf.flip
      out.write(buf)
      buf.clear
    }
  }
}

// -------------------------------------------------------

case class MCommand(args: Array[String], entry: MEntry) {
  def noReply = args.last == "noreply"
  
  def argToLong(at: Int) = itemToLong(args, at)
  
  def argOrElse(at: Int, defaultValue: String) = 
    if (args.length > at)
      args(at)
    else
      defaultValue
}

// -------------------------------------------------------

abstract class MResponse {
  def size: Int
  def put(buf: IoBuffer): Unit
}

case class MResponseLine(line: String) extends MResponse {
  def size = line.length + CRNL.length

  def put(buf: IoBuffer) {
    buf.put(line.toString.getBytes) // TODO: Need a charset here?
    buf.put(CRNLBytes)
  }
}

case class MResponseLineEntry(line: String, entry: MEntry) extends MResponse {
  def size = line.length    + CRNL.length +
             entry.dataSize + CRNL.length

  def put(buf: IoBuffer) {
    buf.put(line.toString.getBytes) // TODO: Need a charset here?
    buf.put(CRNLBytes)
    buf.put(entry.data)
    buf.put(CRNLBytes)
  }
}

// -------------------------------------------------------

class MStats(var set_cmds: Long,
             var get_cmds: Long,
             var get_hits: Long,
             var get_misses: Long) {
  def this() = this(0, 0, 0, 0)
}
