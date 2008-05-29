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

import ff.actord.Util._
import ff.actord.MProtocol._

trait MSession {
  def ident: Long
  def close: Unit

  def read: Byte
  def read(bytes: Array[Byte]): Unit
  def write(bytes: Array[Byte]): Unit = write(bytes, 0, bytes.length)
  def write(bytes: Array[Byte], offset: Int, length: Int): Unit

  def numMessages: Long // Number of messages processed on this session so far.
}

/**
 * Represents a specification, of a command in the protocol.
 *
 * TODO: Need a better process() signature if we want to handle async streaming?
 * TODO: Research how mina allows request and response streaming.
 * TODO: Is there an equivalent of writev/readv in mina?
 */
case class MSpec(line: String, process: (MCommand) => Unit) {
  val args      = line.split(" ")
  val name      = args(0)
  val nameBytes = stringToArray(name)
  val minArgs   = args.filter(_.startsWith("<")).length
  val argsRest  = args.toList.tail

  def checkArgs(a: Seq[String]) = a.length >= minArgs

  val pos_dataSize = argsRest.indexOf("<dataSize>")
  val pos_expTime  = argsRest.indexOf("<expTime>")
  val pos_cas      = Math.max(argsRest.indexOf("<cid>"), argsRest.indexOf("[cid]"))

  def dataSizeParse(cmdArgs: Seq[String]) = itemToInt(cmdArgs, pos_dataSize, -1)
  def expTimeParse(cmdArgs: Seq[String])  = itemToLong(cmdArgs, pos_expTime,  0L)
  def casParse(cmdArgs: Seq[String])      = itemToLong(cmdArgs, pos_cas, -1L)
}

// -------------------------------------------------------

/**
 * The text protocol is defined at:
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
 *
 * This class should be networking implementation independent.  You
 * should not find any java IO or NIO or mina or grizzly words here.
 */
object MProtocol {
  val OK           = stringToArray("OK"         + CRNL)
  val END          = stringToArray("END"        + CRNL)
  val DELETED      = stringToArray("DELETED"    + CRNL)
  val NOT_FOUND    = stringToArray("NOT_FOUND"  + CRNL)
  val NOT_STORED   = stringToArray("NOT_STORED" + CRNL)
  val STORED       = stringToArray("STORED"     + CRNL)
}

trait MProtocol {
  val createdAt = System.currentTimeMillis

  /**
   * Commands defined with a single line.
   * More popular commands should be listed first.
   */
  def oneLineSpecs: List[MSpec] = Nil
           
  /**
   * Commands that use a two lines, such as a line followed by byte data.
   * More popular commands should be listed first.
   */
  def twoLineSpecs: List[MSpec] = Nil
      
  // ----------------------------------------

  val oneLineSpecLookup = indexSpecs(oneLineSpecs)
  val twoLineSpecLookup = indexSpecs(twoLineSpecs)
                          
  // ----------------------------------------

  def indexSpecs(specs: List[MSpec]): Array[List[MSpec]] = {
    val lookup = new Array[List[MSpec]](256) // A lookup table by first character of spec.name.
    for (i <- 0 until lookup.length)
      lookup(i) = Nil                        // Buckets in the lookup table are just Lists.
    for (spec <- specs) {
      val index = spec.name(0) - 'A'
      lookup(index) = lookup(index) ::: List(spec) // Concat so that more popular commands come first.
    }
    lookup
  }

  def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
    lookup(x(0) - 'A').find(spec => arrayCompare(spec.nameBytes, spec.nameBytes.length, x, xLen) == 0)

  // ----------------------------------------

  final val GOOD               = 0
  final val SECONDS_IN_30_DAYS = 60*60*24*30

  def processArgs(cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int): Seq[String] = 
    arraySplit(cmdArr, cmdLen + 1, Math.max(cmdArrLen - CRNL.length - (cmdLen + 1), 0), SPACE)

  /**
   * This function is called by the networking implementation
   * when there is incoming data/message that needs to be processed.
   *
   * Returns how many bytes more need to be read before
   * the message can be processed successfully.  Or, just
   * returns 0 (or OK) to mean we've processed the message.
   *
   * cmdArr     - the incoming message command line bytes, including CRNL, and maybe more. 
   * cmdArrLen  - tells us what part of the cmdArr belongs to the command line, including CRNL.
   * readyCount - number of bytes from message start (including cmdArr)
   *              plus remaining data, available to be read.
   */
  def process(session: MSession, 
              cmdArr: Array[Byte], // We do not own the input cmdArr.
              cmdArrLen: Int,      // Number of command line bytes in cmdArr, including CRNL.
              readyCount: Int): Int = {
    if (BENCHMARK_NETWORK_ONLY.shortCircuit(session, cmdArr, cmdArrLen)) 
       return GOOD

    val length = cmdArrLen - CRNL.length
    val spcPos = arrayIndexOf(cmdArr, 0, length, SPACE)
    val cmdLen = if (spcPos > 0) spcPos else length

    findSpec(cmdArr, cmdLen, oneLineSpecLookup).map(
      spec => processOneLine(spec, session, cmdArr, cmdArrLen, cmdLen)
    ) orElse findSpec(cmdArr, cmdLen, twoLineSpecLookup).map(
      spec => {
        // Handle two line message, such as:
        //   <cmdName> <key> <flags> <expTime> <dataSize> [noreply]\r\n
        //         cas <key> <flags> <expTime> <dataSize> <cid_unique> [noreply]\r\n
        //       VALUE <key> <flags> <dataSize> [cid]\r\n
        //
        val cmdArgs = processArgs(cmdArr, cmdArrLen, cmdLen)
        if (spec.checkArgs(cmdArgs)) {
          var dataSize = spec.dataSizeParse(cmdArgs)
          if (dataSize >= 0) {
            val totalNeeded = cmdArrLen + dataSize + CRNL.length
            if (totalNeeded <= readyCount) {
              processTwoLine(spec, session, cmdArr, cmdArrLen, cmdLen, cmdArgs, dataSize)
            } else {
              totalNeeded
            }
          } else {
            session.write(stringToArray("CLIENT_ERROR missing dataSize at " + spec.pos_dataSize + 
                                        " in: " + arrayToString(cmdArr) + CRNL))
            GOOD
          }
        } else {
          session.write(stringToArray("CLIENT_ERROR args: " + arrayToString(cmdArr, 0, length) + CRNL))
          GOOD
        }
      }
    ) getOrElse {
      session.write(stringToArray("ERROR " + arrayToString(cmdArr, 0, cmdLen) + CRNL)) 
      GOOD // Saw an unknown command, but keep going and process the next command.
    }
  }

  def processOneLine(spec: MSpec, 
                     session: MSession, 
                     cmdArr: Array[Byte],
                     cmdArrLen: Int,
                     cmdLen: Int): Int = {
    val cmdArgs = processArgs(cmdArr, cmdArrLen, cmdLen)
    if (spec.checkArgs(cmdArgs))
      spec.process(MCommand(session, cmdArr, cmdLen, cmdArgs, null))
    else
      session.write(stringToArray("CLIENT_ERROR args: " + arrayToString(cmdArr, 0, cmdArrLen)))
    GOOD
  }

  def processTwoLine(spec: MSpec, 
                     session: MSession, 
                     cmdArr: Array[Byte],
                     cmdArrLen: Int,
                     cmdLen: Int,
                     cmdArgs: Seq[String],
                     dataSize: Int): Int = { // Called when we have all the incoming bytes of a two-lined message.
    val data = new Array[Byte](dataSize)

    session.read(data)

    if (session.read == CR &&
        session.read == NL) {
      val expTime = spec.expTimeParse(cmdArgs)

      var cid = spec.casParse(cmdArgs)
      if (cid == -1L)
          cid = ((session.ident << 32) + (session.numMessages & 0xFFFFFFFFL))

      spec.process(MCommand(session, cmdArr, cmdLen, cmdArgs,
                            MEntry(cmdArgs(0), // The <key> == cmdArgs(0) item.
                                   parseLong(cmdArgs(1), 0L),
                                   if (expTime != 0L &&
                                       expTime <= SECONDS_IN_30_DAYS)
                                       expTime + nowInSeconds
                                   else
                                       expTime,
                                   data,
                                   cid)))
    } else
      session.write(stringToArray("CLIENT_ERROR missing CRNL after data" + CRNL))
    GOOD
  }
}

// -------------------------------------------

class MProtocolServer(svr: MServer) extends MProtocol {
  override def oneLineSpecs = List( 
      MSpec("get <key>*",
            (cmd) => { 
if (!BENCHMARK_NETWORK_ONLY.shortCircuitGet(cmd)) {
              svr.get(cmd.args).
                  foreach(el => cmd.write(el, false))
              cmd.reply(END)
}
            }),

      MSpec("gets <key>*",
            (cmd) => {
              svr.get(cmd.args).
                  foreach(el => cmd.write(el, true))
              cmd.reply(END)
            }),

      MSpec("delete <key> [<time>] [noreply]",
            (cmd) => 
              cmd.reply(svr.delete(cmd.args(0), cmd.argToLong(1), cmd.noReply), 
                        DELETED, NOT_FOUND)),

      MSpec("incr <key> <value> [noreply]",
            (cmd) => cmd.reply(svr.delta(cmd.args(0),  cmd.argToLong(1), cmd.noReply))),
      MSpec("decr <key> <value> [noreply]",
            (cmd) => cmd.reply(svr.delta(cmd.args(0), -cmd.argToLong(1), cmd.noReply))),
           
      MSpec("stats [<arg>]",
            (cmd) => 
              cmd.reply(stats(svr, cmd.argOrElse(0, null)))),

      MSpec("flush_all [<delay>] [noreply]",
            (cmd) => {
               svr.flushAll(cmd.argToLong(0))
               cmd.reply(OK)
             }),

      MSpec("version", 
            (cmd) => cmd.reply(stringToArray("VERSION " + MServer.version + CRNL))),
      MSpec("verbosity",
            (cmd) => cmd.reply(OK)), // TODO: verbosity command.
      MSpec("quit",
            (cmd) => cmd.session.close),

      // Extensions to basic protocol.
      //
      MSpec("range <key_from> <key_to>", // The key_from is inclusive lower-bound, key_to is exclusive upper-bound.
            (cmd) => { 
              svr.range(cmd.args(0), cmd.args(1)).
                  foreach(el => cmd.write(el, false))
              cmd.reply(END)
            }))
           
  override def twoLineSpecs = List( 
      MSpec("set <key> <flags> <expTime> <dataSize> [noreply]",
            (cmd) => 
               cmd.reply(svr.set(cmd.entry, cmd.noReply), 
                         STORED, NOT_STORED)),

      MSpec("add <key> <flags> <expTime> <dataSize> [noreply]",
            (cmd) => 
               cmd.reply(svr.addRep(cmd.entry, true, cmd.noReply), 
                         STORED, NOT_STORED)),

      MSpec("replace <key> <flags> <expTime> <dataSize> [noreply]",
            (cmd) => 
               cmd.reply(svr.addRep(cmd.entry, false, cmd.noReply), 
                         STORED, NOT_STORED)),

      MSpec("append <key> <flags> <expTime> <dataSize> [noreply]",
            (cmd) => 
               cmd.reply(svr.xpend(cmd.entry, true, cmd.noReply), 
                         STORED, NOT_STORED)),

      MSpec("prepend <key> <flags> <expTime> <dataSize> [noreply]",
            (cmd) => 
               cmd.reply(svr.xpend(cmd.entry, false, cmd.noReply), 
                         STORED, NOT_STORED)),
           
      MSpec("cas <key> <flags> <expTime> <dataSize> <cid_unique> [noreply]", // Using <cid_unique>, not <cid>, for generated cid.
            (cmd) => 
               cmd.reply(stringToArray(svr.checkAndSet(cmd.entry, cmd.argToLong(4), cmd.noReply) + CRNL))),

      // Extensions to basic protocol.
      //
      MSpec("act <key> <flags> <expTime> <dataSize> [noreply]", // Like RPC, but meant to call a registered actor.
            (cmd) => {
               svr.act(cmd.entry, cmd.noReply).
                   foreach(el => cmd.write(el, false))
               cmd.reply(END)
            }))
      
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

      statLine("version", MServer.version)

      statLine("cmd_gets",   String.valueOf(svrStats.cmd_gets))
      statLine("cmd_sets",   String.valueOf(svrStats.cmd_sets))
      statLine("get_hits",   String.valueOf(svrStats.get_hits))
      statLine("get_misses", String.valueOf(svrStats.get_misses))

//    statLine("curr_connections",  String.valueOf(curr_conns))
//    statLine("total_connections", String.valueOf(total_conns))
//    statLine("bytes_read",    0.toString)
//    statLine("bytes_written", 0.toString)

      val ctm = System.currentTimeMillis

      statLine("time",   (new java.util.Date()) + " " + ctm.toString)
      statLine("uptime", (ctm - createdAt).toString)

      statLine("curr_items",     svrStats.numEntries.toString)
      statLine("evictions",      svrStats.evictions.toString)
      statLine("bytes",          svrStats.usedMemory.toString)
//    statLine("limit_maxbytes", svr.limitMemory.toString)
      statLine("current_bytes",  Runtime.getRuntime.totalMemory.toString)
      statLine("free_bytes",     Runtime.getRuntime.freeMemory.toString)

//    statLine("pid",           0.toString)
//    statLine("pointer_size",  0.toString)
//    statLine("rusage_user",   "0:0")
//    statLine("rusage_system", "0:0")
//    statLine("threads",       0.toString)
//    statLine("connection_structures", 0.toString)
    }

    sb.append("END\r\n")
    stringToArray(sb.toString)
  }

/*
    Info on how stats should work, from memcached protocol.txt...
    
    Name              Type     Meaning
    ----------------------------------
    n/a or unknown...
      connection_structures 32u  Number of connection structures allocated by the server
    o.s. or not available in pure scala/java...
      pid               32u      Process id of this server process
      pointer_size      32       Default size of pointers on the host OS (generally 32 or 64)
      rusage_user       32u:32u  Accumulated user time for this process (seconds:microseconds)
      rusage_system     32u:32u  Accumulated system time for this process 
                                 (seconds:microseconds) ever since it started
    global...
      time              32u      current UNIX time according to the server
    mina...
      curr_connections  32u      Number of open connections
      total_connections 32u      Total number of connections opened since 
                                 the server started running
      bytes_read        64u      Total number of bytes read by this server from network
      bytes_written     64u      Total number of bytes sent by this server to network
      threads           32u      Number of worker threads requested.
                                 (see doc/threads.txt)
    session...
      cmd_get           64u      Cumulative number of retrieval requests
      cmd_set           64u      Cumulative number of storage requests
      get_hits          64u      Number of keys that have been requested and found present
      get_misses        64u      Number of items that have been requested and not found
    subServer...                           
      curr_items        32u      Current number of items stored by the server
      total_items       32u      Total number of items stored by this server 
        note: not sure what's the difference between total_items and cmd_set        
      bytes             64u      Current number of bytes used by this server to store items
      evictions         64u      Number of valid items removed from cache
                                 to free memory for new items
    server...
      version           string   Version string of this server
      uptime            32u      Number of seconds this server has been running
      limit_maxbytes    32u      Number of bytes this server is allowed to use for storage
*/
}

// -------------------------------------------------------

/**
 * Represents an incoming command or request from a (remote) client.
 */
case class MCommand(session: MSession, cmdArr: Array[Byte], cmdLen: Int, args: Seq[String], entry: MEntry) {
  val noReply = args.length > 0 && args.last == "noreply"
  
  def argToLong(at: Int) = itemToLong(args, at)
  
  def argOrElse(at: Int, defaultValue: String) = 
    if (args.length > at)
      args(at)
    else
      defaultValue
      
  override def toString = 
    args.mkString(" ") + (if (entry != null) (" " + entry.toString) else "")

  // ----------------------------------------

  def reply(x: Array[Byte]): Unit = 
   if (!noReply) 
     session.write(x)
           
  def reply(v: Boolean, t: Array[Byte], f: Array[Byte]): Unit =
    reply(if (v) t else f)
      
  def reply(v: Long): Unit = // Used by incr/decr processing.
    if (!noReply)
      reply(if (v < 0)
              NOT_FOUND
            else
              stringToArray(v.toString + CRNL))

  /**
   * A VALUE response of a String of a single line, followed by a single MEntry data bytes.
   */
  def write(entry: MEntry, withCAS: Boolean): Unit =
    if (!noReply) {
      val line: Array[Byte] = {
        val x = entry.comm.asInstanceOf[Array[Byte]]
        if (x != null)
            x
        else
            entry.comm_!(stringToArray("VALUE " + entry.key + " " + entry.flags + " " + entry.data.size)).
                  asInstanceOf[Array[Byte]]
      }
      session.write(line)
      if (withCAS) {
        session.write(SPACEBytes)
        session.write(stringToArray(entry.cid.toString))
      }
      session.write(CRNLBytes)
      session.write(entry.data)
      session.write(CRNLBytes)
    }
}

///////////////////////////////////////////////// 
object BENCHMARK_NETWORK_ONLY { 
  val manyBytes = new Array[Byte](400) // 400 matches memslap default data length.
  for (i <- 0 until 400)
    manyBytes(i) = 'a'.asInstanceOf[Byte]
  val entry = MEntry(null, 0, 0, manyBytes, 0L)
  val GByte = 'g'.asInstanceOf[Byte]
  val valBeg = stringToArray("VALUE ")
  val valEnd = stringToArray(" 0 400\r\n")

  def shortCircuit(session: MSession, cmdArr: Array[Byte], cmdArrLen: Int): Boolean = { 
    // Return true to benchmark just the networking layers, not the in-memory or persistent storage or findSpec.
    return false
    if (cmdArr(0) != GByte) // Do a short circuit only for 'get' messages.
      return false

    val len = cmdArrLen - CRNL.length
    val k = arrayIndexOf(cmdArr, 0, len, SPACE) + 1
    session.write(valBeg)
    session.write(cmdArr, k, len - k)
    session.write(valEnd)
    session.write(entry.data)
    session.write(CRNLBytes)
    session.write(END)
    true
  }

  def shortCircuitGet(cmd: MCommand): Boolean = {
    // Return true to benchmark just the networking layers around a get, not the in-memory or persistent storage.
    return false
    val session = cmd.session
    session.write(valBeg)
    session.write(stringToArray(cmd.args(0)))
    session.write(valEnd)
    session.write(entry.data)
    session.write(CRNLBytes)
    session.write(END)
    true
  }
}
/////////////////////////////////////////////////

