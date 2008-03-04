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
 * TODO: Research how mina allows request and response streaming.
 * TODO: Is there an equivalent of writev/readv in mina?
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
             svr.getMulti(cmd.args.slice(1, cmd.args.length)).
                 foreach(el => sess.write(List(MResponseLineEntry(asValueLine(el), el))))
             reply("END")
           }),

      Spec("gets <key>*",
           (svr, cmd, sess) => {
             svr.getMulti(cmd.args.slice(1, cmd.args.length)).
                 foreach(el => sess.write(List(MResponseLineEntry(asValueLineCAS(el), el))))
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
           
      Spec("stats [<arg>]",
           (svr, cmd, sess) => 
             reply(stats(svr, cmd.argOrElse(1, null)))),

      Spec("flush_all [<delay>] [noreply]",
           (svr, cmd, sess) => {
              svr.flushAll(cmd.argToLong(1))
              reply("OK")
            }),

      Spec("version",
           (svr, cmd, sess) => reply("VERSION " + svr.version)),
      Spec("verbosity",
           (svr, cmd, sess) => reply("OK")),
      Spec("quit",
           (svr, cmd, sess) => {
             sess.close
             Nil
           }),

      // Extensions to basic protocol.
      //
      Spec("range <key_from> <key_to>", // key_from is inclusive, key_to is exclusive
           (svr, cmd, sess) => { 
             svr.range(cmd.args(1), cmd.args(2),
                       el => sess.write(List(MResponseLineEntry(asValueLine(el), el))))
             reply("END")
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
            
            // TODO: Handle this better when dataSize is huge.
            //       Perhaps use mmap, or did mina read it entirely 
            //       into memory by this point already?
            //
            val data = new Array[Byte](dataSize) 
  
            in.get(data)
            
            if (in.getString(CRNL.length, charsetDecoder) == CRNL) {
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
              out.write(MResponseLine("CLIENT_ERROR missing CRNL after data"))
              MessageDecoderResult.OK
            }
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

	def stats(svr: MServer, arg: String) = {
	  var sb = new StringBuffer
	  
	  def statLine(k: String, v: String) = {
	    sb.append("STAT ")
	    sb.append(k)
	    sb.append(" ")		    
	    sb.append(v)		    
	    sb.append(CRNL)
	  }
		
		if (arg == "keys") {
		  for (key <- svr.keys)
		    statLine("key", key)
		} else {
		  val svrStats = svr.stats
		
      statLine("version", svr.version)

//    statLine("cmd_gets",   String.valueOf(get_cmds))
//    statLine("cmd_sets",   String.valueOf(set_cmds))
//    statLine("get_hits",   String.valueOf(get_hits))
//    statLine("get_misses", String.valueOf(get_misses))

//    statLine("curr_connections",  String.valueOf(curr_conns))
//    statLine("total_connections", String.valueOf(total_conns))
//    statLine("bytes_read",    0.toString)
//    statLine("bytes_written", 0.toString)

      statLine("time",   new java.util.Date() + " " + System.currentTimeMillis)
      statLine("uptime", (System.currentTimeMillis - svr.createdAt).toString)

      statLine("curr_items",     svrStats.numEntries.toString)
      statLine("evictions",      svrStats.evictions.toString)
      statLine("bytes",          svrStats.usedMemory.toString)
      statLine("limit_maxbytes", svr.limitMemory.toString)
      statLine("current_bytes",  Runtime.getRuntime.totalMemory.toString)
      statLine("free_bytes",     Runtime.getRuntime.freeMemory.toString)
      
//    statLine("pid",           0.toString)
//    statLine("pointer_size",  0.toString)
//    statLine("rusage_user",   "0:0")
//    statLine("rusage_system", "0:0")
//    statLine("threads",       0.toString)
//    statLine("connection_structures", 0.toString)
    }

    sb.append("END")
    sb.toString
	}

/*
Name              Type     Meaning
----------------------------------
n/a or unknown...
  connection_structures 32u  Number of connection structures allocated 
                             by the server
o.s. or not available in pure java...
  pid               32u      Process id of this server process
  pointer_size      32       Default size of pointers on the host OS
                             (generally 32 or 64)
  rusage_user       32u:32u  Accumulated user time for this process 
                             (seconds:microseconds)
  rusage_system     32u:32u  Accumulated system time for this process 
                             (seconds:microseconds) ever since it started
global...
  time              32u      current UNIX time according to the server
mina...
  curr_connections  32u      Number of open connections
  total_connections 32u      Total number of connections opened since 
                             the server started running
  bytes_read        64u      Total number of bytes read by this server 
                             from network
  bytes_written     64u      Total number of bytes sent by this server to 
                             network
  threads           32u      Number of worker threads requested.
                             (see doc/threads.txt)
session...
  cmd_get           64u      Cumulative number of retrieval requests
  cmd_set           64u      Cumulative number of storage requests
  get_hits          64u      Number of keys that have been requested and 
                             found present
  get_misses        64u      Number of items that have been requested 
                             and not found
subServer...                           
  curr_items        32u      Current number of items stored by the server
  total_items       32u      Total number of items stored by this server 
    note: not sure what's the difference between total_items and cmd_set
    
  bytes             64u      Current number of bytes used by this server 
                             to store items
  evictions         64u      Number of valid items removed from cache                                                                           
                             to free memory for new items                                                                                       
server...
  version           string   Version string of this server
  uptime            32u      Number of seconds this server has been running
  limit_maxbytes    32u      Number of bytes this server is allowed to
                             use for storage. 
*/
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
      
  override def toString = 
    args.mkString(" ") + (if (entry != null) (" " + entry.toString) else "")
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
