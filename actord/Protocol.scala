package ff.actord

import java.net._
import java.nio.charset._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

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
             (1 until cmd.args.length).flatMap(
               i => svr.get(cmd.args(i)).
                        map(el => MResponseLineEntry(asValueLine(el), el)).
                        toList
             ).toList ::: reply("END")
           }),

//    Spec("gets <key>*",
//         (svr, cmd, sess) => {
//           (1 until cmd.args.length).flatMap(
//             i => svr.get(cmd.args(i)).
//                      map(el => MResponseLineEntry(asValueLineCAS(el), el)).
//                      toList
//           ).toList ::: reply("END")
//         }),

      Spec("delete <key> [<time>] [noreply]",
           (svr, cmd, sess) => 
             reply(svr.delete(cmd.args(1), cmd.argToLong(2)), 
                   "DELETED", "NOT_FOUND")),

      Spec("incr <key> <value> [noreply]",
           (svr, cmd, sess) => reply(svr.delta(cmd.args(1), cmd.argToLong(2)))),
      Spec("decr <key> <value> [noreply]",
           (svr, cmd, sess) => reply(svr.delta(cmd.args(1), -1L * cmd.argToLong(2)))),
           
      Spec("stats [<arg>]",
           (svr, cmd, sess) => 
             reply(svr.stats(cmd.argOrElse(1, null)))),

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
              reply(svr.set(cmd.entry), 
                    "STORED", "NOT_STORED")),

      Spec("add <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.add(cmd.entry), 
                    "STORED", "NOT_STORED")),

      Spec("replace <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.replace(cmd.entry), 
                    "STORED", "NOT_STORED"))

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

  def asValueLine(e: MEntry) =
      "VALUE " + e.key + " " + e.flags + " " + e.dataSize
  
  def asValueLineCAS(e: MEntry) =
      asValueLine(e) + " " + e.cas
  
  // ----------------------------------------

  def reply(s: String): List[MResponse] = List(MResponseLine(s))
           
  def reply(v: Boolean, t: String, f: String): List[MResponse] =
      reply(if (v) t else f)
                          
  // ----------------------------------------

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
      buf.reset
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

