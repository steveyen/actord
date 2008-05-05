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
case class Spec(line: String, process: (MServer, MCommand) => Unit) {
  val args      = line.split(" ")
  val name      = args(0)
  val nameBytes = name.getBytes
  val minArgs   = args.filter(_.startsWith("<")).length

  def checkArgs(a: Seq[String]) = a.length >= minArgs
}

// -------------------------------------------------------

/**
 * Protocol is defined at:
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
 *
 * This class should be networking implementation independent.  You
 * should not find any java IO or NIO or mina or grizzly words here.
 */
object MProtocol {
  val OK           = ("OK"         + CRNL).getBytes
  val END          = ("END"        + CRNL).getBytes
  val DELETED      = ("DELETED"    + CRNL).getBytes
  val NOT_FOUND    = ("NOT_FOUND"  + CRNL).getBytes
  val NOT_STORED   = ("NOT_STORED" + CRNL).getBytes
  val STORED       = ("STORED"     + CRNL).getBytes
}

class MProtocol {
  val createdAt = System.currentTimeMillis

  /**
   * Commands defined with a single line.
   * More popular commands should be listed first.
   * Subclasses might override this list to add custom commands.
   */
  def singleLineSpecs = List( 
      Spec("get <key>*",
           (svr, cmd) => { 
             svr.get(cmd.args).
                 foreach(el => cmd.write(el, false))
             cmd.reply(END)
           }),

      Spec("gets <key>*",
           (svr, cmd) => {
             svr.get(cmd.args).
                 foreach(el => cmd.write(el, true))
             cmd.reply(END)
           }),

      Spec("delete <key> [<time>] [noreply]",
           (svr, cmd) => 
             cmd.reply(svr.delete(cmd.args(0), cmd.argToLong(1), cmd.noReply), 
                       DELETED, NOT_FOUND)),

      Spec("incr <key> <value> [noreply]",
           (svr, cmd) => cmd.reply(svr.delta(cmd.args(0),  cmd.argToLong(1), cmd.noReply))),
      Spec("decr <key> <value> [noreply]",
           (svr, cmd) => cmd.reply(svr.delta(cmd.args(0), -cmd.argToLong(1), cmd.noReply))),
           
      Spec("stats [<arg>]",
           (svr, cmd) => 
             cmd.reply(stats(svr, cmd.argOrElse(0, null)))),

      Spec("flush_all [<delay>] [noreply]",
           (svr, cmd) => {
              svr.flushAll(cmd.argToLong(0))
              cmd.reply(OK)
            }),

      Spec("version", 
           (svr, cmd) => cmd.reply(("VERSION " + MServer.version + CRNL).getBytes)),
      Spec("verbosity",
           (svr, cmd) => cmd.reply(OK)), // TODO: verbosity command.
      Spec("quit",
           (svr, cmd) => cmd.session.close),

      // Extensions to basic protocol.
      //
      Spec("range <key_from> <key_to>", // The key_from is inclusive lower-bound, key_to is exclusive upper-bound.
           (svr, cmd) => { 
             svr.range(cmd.args(0), cmd.args(1)).
                 foreach(el => cmd.write(el, false))
             cmd.reply(END)
           }))
           
  /**
   * Commands that use a multiple lines, such as a line followed by byte data.
   * More popular commands should be listed first.
   * Subclasses might override this list to add custom commands.
   */
  def multiLineSpecs = List( 
      Spec("set <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd) => 
              cmd.reply(svr.set(cmd.entry, cmd.noReply), 
                        STORED, NOT_STORED)),

      Spec("add <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd) => 
              cmd.reply(svr.addRep(cmd.entry, true, cmd.noReply), 
                        STORED, NOT_STORED)),

      Spec("replace <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd) => 
              cmd.reply(svr.addRep(cmd.entry, false, cmd.noReply), 
                        STORED, NOT_STORED)),

      Spec("append <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd) => 
              cmd.reply(svr.xpend(cmd.entry, true, cmd.noReply), 
                        STORED, NOT_STORED)),

      Spec("prepend <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd) => 
              cmd.reply(svr.xpend(cmd.entry, false, cmd.noReply), 
                        STORED, NOT_STORED)),
           
      Spec("cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]",
           (svr, cmd) => 
              cmd.reply((svr.checkAndSet(cmd.entry, cmd.argToLong(4), cmd.noReply) + CRNL).getBytes)),

      // Extensions to basic protocol.
      //
      Spec("act <key> <flags> <expTime> <bytes> [noreply]", // Like RPC, but meant to call a registered actor.
           (svr, cmd) => {
              svr.act(cmd.entry, cmd.noReply).
                  foreach(el => cmd.write(el, false))
              cmd.reply(END)
           }))
      
  val singleLineSpecLookup = indexSpecs(singleLineSpecs)
  val multiLineSpecLookup  = indexSpecs(multiLineSpecs)
                          
  // ----------------------------------------

  def indexSpecs(specs: List[Spec]): Array[List[Spec]] = {
    val lookup = new Array[List[Spec]](26) // A perfect hash lookup table, by first character of spec.name.
    for (i <- 0 until lookup.length)
      lookup(i) = Nil
    specs.map(spec => {
      val index = spec.name(0) - 'a'
      lookup(index) = lookup(index) ::: List(spec) // Concat so that more popular commands come first.
    })
    lookup
  }

  def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[Spec]]): Option[Spec] = 
    lookup(x(0) - 'a').find(spec => arrayCompare(spec.nameBytes, spec.nameBytes.length, x, xLen) == 0)

  // ----------------------------------------

  final val GOOD               = 0
  final val SECONDS_IN_30_DAYS = 60*60*24*30

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
  def process(server: MServer,
              session: MSession, 
              cmdArr: Array[Byte], // We do not own the input cmdArr.
              cmdArrLen: Int,      // Number of command line bytes in cmdArr, including CRNL.
              readyCount: Int): Int = {
if (BENCHMARK_NETWORK_ONLY.shortCircuit(session, cmdArr, cmdArrLen)) return GOOD

    val length     = cmdArrLen - CRNL.length
    val spcPos     = indexOfByte(cmdArr, 0, length, SPACE)
    val cmdLen     = if (spcPos > 0) spcPos else length
    val cmdArgsLen = Math.max(length - (cmdLen + 1), 0)
    val cmdArgs    = splitArray(cmdArr, cmdLen + 1, cmdArgsLen)

    findSpec(cmdArr, cmdLen, singleLineSpecLookup).map(
      spec => {
        if (spec.checkArgs(cmdArgs)) {
          spec.process(server, MCommand(session, cmdArr, cmdLen, cmdArgs, null))
          GOOD
        } else {
          session.write(("CLIENT_ERROR args: " + (new String(cmdArr, 0, length)) + CRNL).getBytes)
          GOOD
        }
      }
    ) orElse findSpec(cmdArr, cmdLen, multiLineSpecLookup).map(
      spec => {
        // Handle mutator command: 
        //   <cmdName> <key> <flags> <expTime> <bytes> [noreply]\r\n
        //   cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]\r\n
        //
        if (spec.checkArgs(cmdArgs)) {
          var dataSize    = cmdArgs(3).toInt
          val totalNeeded = cmdArrLen + dataSize + CRNL.length
          if (totalNeeded <= readyCount) {
            val expTime = cmdArgs(2).toLong
            
            // TODO: Handle this better when dataSize is huge.
            //       Perhaps use mmap, or did mina read it entirely 
            //       into memory by this point already?
            //
            val data = new Array[Byte](dataSize)
            
            session.read(data)
            
            if (session.read == CR &&
                session.read == NL) {
              spec.process(server, 
                           MCommand(session, cmdArr, cmdLen, cmdArgs,
                                    MEntry(cmdArgs(0),
                                           cmdArgs(1).toLong,
                                           if (expTime != 0 &&
                                               expTime <= SECONDS_IN_30_DAYS)
                                               expTime + nowInSeconds
                                           else
                                               expTime,
                                           data,
                                           (session.ident << 32) + 
                                           (session.numMessages & 0xFFFFFFFFL))))
              GOOD
            } else {
              session.write(("CLIENT_ERROR missing CRNL after data" + CRNL).getBytes)
              GOOD
            }
          } else {
            totalNeeded
          }
        } else {
          session.write(("CLIENT_ERROR args: " + (new String(cmdArr, 0, length)) + CRNL).getBytes)
          GOOD
        }
      }
    ) getOrElse {
      session.write(("ERROR " + (new String(cmdArr, 0, cmdLen)) + CRNL).getBytes) 
      GOOD // Saw an unknown command, but keep going and process the next command.
    }
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
    sb.toString.getBytes
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
              (v.toString + CRNL).getBytes)

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
            entry.comm_!(("VALUE " + entry.key + " " + entry.flags + " " + entry.data.size).getBytes).
                  asInstanceOf[Array[Byte]]
      }
      session.write(line)
      if (withCAS) {
        session.write(SPACEBytes)
        session.write(entry.cid.toString.getBytes)
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
  val valBeg = "VALUE ".getBytes
  val valEnd = " 0 400\r\n".getBytes

  def shortCircuit(session: MSession, cmdArr: Array[Byte], cmdArrLen: Int): Boolean = { 
    // Return true to benchmark just the networking layers, not the in-memory or persistent storage.
    return false

    if (cmdArr(0) != GByte) // Do a short circuit only for 'get' messages.
      return false

    val len = cmdArrLen - CRNL.length
    val k = indexOfByte(cmdArr, 0, len, SPACE) + 1
    session.write(valBeg)
    session.write(cmdArr, k, len - k)
    session.write(valEnd)
    session.write(entry.data)
    session.write(CRNLBytes)
    session.write(END)
    true
  }
}
/////////////////////////////////////////////////

