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

import ff.actord.Util._

trait MBufferIn {
  def read(bytes: Array[Byte]): Unit
  def readString(num: Int): String
}

trait MBufferOut {
  def put(bytes: Array[Byte]): Unit
}

trait MSession {
  def getId: Long
  def close: Unit
  def write(r: MResponse): Unit
  def getReadMessages: Long
}

/**
 * Represents a specification, of a command in the protocol.
 *
 * TODO: Need a better process() signature if we want to handle async streaming?
 * TODO: Research how mina allows request and response streaming.
 * TODO: Is there an equivalent of writev/readv in mina?
 */
case class Spec(line: String,
                process: (MServer, MCommand, MSession) => MResponse) {
  val args = line.split(" ")
  val name = args(0)
  
  val minArgs = 1 + args.filter(_.startsWith("<")).length

  def checkArgs(a: Seq[String]) = a.length >= minArgs
}

// -------------------------------------------------------

/**
 * Protocol is defined at:
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
 */
class MProtocol {
  val OK         = "OK"         + CRNL
  val END        = "END"        + CRNL
  val DELETED    = "DELETED"    + CRNL
  val NOT_FOUND  = "NOT_FOUND"  + CRNL
  val NOT_STORED = "NOT_STORED" + CRNL
  val STORED     = "STORED"     + CRNL

  /**
   * Commands defined with a single line.
   *
   * Subclasses might override this list to add custom commands.
   */
  def lineOnlySpecs = List( 
      Spec("get <key>*",
           (svr, cmd, sess) => { 
             svr.get(cmd.args.slice(1, cmd.args.length)).
                 foreach(el => sess.write(MResponseLineEntry(asValueLine(el), el)))
             reply(END)
           }),

      Spec("gets <key>*",
           (svr, cmd, sess) => {
             svr.get(cmd.args.slice(1, cmd.args.length)).
                 foreach(el => sess.write(MResponseLineEntry(asValueLineCAS(el), el)))
             reply(END)
           }),

      Spec("delete <key> [<time>] [noreply]",
           (svr, cmd, sess) => 
             reply(svr.delete(cmd.args(1), cmd.argToLong(2), cmd.noReply), 
                   DELETED, NOT_FOUND)),

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
              reply(OK)
            }),

      Spec("version",
           (svr, cmd, sess) => reply("VERSION " + svr.version + CRNL)),
      Spec("verbosity",
           (svr, cmd, sess) => reply(OK)),
      Spec("quit",
           (svr, cmd, sess) => {
             sess.close
             reply("")
           }),

      // Extensions to basic protocol.
      //
      Spec("range <key_from> <key_to>", // The key_from is inclusive lower-bound, key_to is exclusive upper-bound.
           (svr, cmd, sess) => { 
             svr.range(cmd.args(1), cmd.args(2)).
                 foreach(el => sess.write(MResponseLineEntry(asValueLine(el), el)))
             reply(END)
           }))
           
  /**
   * Commands that use a line followed by byte data.
   *
   * Subclasses might override this list to add custom commands.
   */
  def lineWithDataSpecs = List( 
      Spec("set <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.set(cmd.entry, cmd.noReply), 
                    STORED, NOT_STORED)),

      Spec("add <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.add(cmd.entry, cmd.noReply), 
                    STORED, NOT_STORED)),

      Spec("replace <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.replace(cmd.entry, cmd.noReply), 
                    STORED, NOT_STORED)),

      Spec("append <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.xpend(cmd.entry, true, cmd.noReply), 
                    STORED, NOT_STORED)),

      Spec("prepend <key> <flags> <expTime> <bytes> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.xpend(cmd.entry, false, cmd.noReply), 
                    STORED, NOT_STORED)),
           
      Spec("cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]",
           (svr, cmd, sess) => 
              reply(svr.checkAndSet(cmd.entry, cmd.argToLong(5), cmd.noReply))),

      // Extensions to basic protocol.
      //
      Spec("act <key> <flags> <expTime> <bytes> [noreply]", // Like RPC, but meant to call a registered actor.
           (svr, cmd, sess) => {
              svr.act(cmd.entry, cmd.noReply).
                  foreach(el => sess.write(MResponseLineEntry(asValueLine(el), el)))
              reply(END)
           }))
      
  val lineOnlyCommands = 
      Map[String, Spec](lineOnlySpecs.
                          map(s => Pair(s.name, s)):_*)

  val lineWithDataCommands = 
      Map[String, Spec](lineWithDataSpecs.
                          map(s => Pair(s.name, s)):_*)
                          
  // ----------------------------------------

  def asValueLine(e: MEntry) =
      "VALUE " + e.key + " " + e.flags + " " + e.dataSize + CRNL
  
  def asValueLineCAS(e: MEntry) =
      "VALUE " + e.key + " " + e.flags + " " + e.dataSize + " " + e.cid + CRNL
  
  // ----------------------------------------

  def reply(s: String): MResponse = MResponseLine(s)
           
  def reply(v: Boolean, t: String, f: String): MResponse =
      reply(if (v) t else f)
      
  def reply(v: Long): MResponse = // Used by incr/decr processing.
      reply(if (v < 0)
              NOT_FOUND
            else
              (v.toString + CRNL))
                          
  // ----------------------------------------

  val SECONDS_IN_30_DAYS = 60*60*24*30

  val GOOD = 0

  /**
   * Returns how many bytes more need to be read before
   * the message can be processed successfully.  Or, just
   * returns 0 (or OK) to mean we've processed the message.
   *
   * cmdLine    - the incoming message command line. 
   * cmdData    - the secondary data/value part of the message, if any.
   * readyCount - number of bytes from message start (including cmdLine) 
   *              plus remaining data, available to be read.
   */
  def process(server: MServer,
              session: MSession, 
              cmdLine: String,   
              cmdData: MBufferIn,
              readyCount: Int): Int = {
    val cmdArgs = cmdLine.trim.split(" ")             
    val cmdName = cmdArgs(0)

    lineOnlyCommands.get(cmdName).map(
      spec => {
        if (spec.checkArgs(cmdArgs)) {
          val cmd = MCommand(cmdArgs, null)
          val res = spec.process(server, cmd, session)
          if (cmd.noReply == false)
            session.write(res)
          GOOD
        } else {
          session.write(MResponseLine("CLIENT_ERROR args: " + cmdArgs.mkString(" ") + CRNL))
          GOOD
        }
      }
    ) orElse lineWithDataCommands.get(cmdName).map(
      spec => {
        // Handle mutator command: 
        //   <cmdName> <key> <flags> <expTime> <bytes> [noreply]\r\n
        //   cas <key> <flags> <expTime> <bytes> <cas_unique> [noreply]\r\n
        //
        if (spec.checkArgs(cmdArgs)) {
          var dataSize    = cmdArgs(4).trim.toInt
          val totalNeeded = cmdLine.length + dataSize + CRNL.length
          if (totalNeeded <= readyCount) {
            val expTime = cmdArgs(3).trim.toLong
            
            // TODO: Handle this better when dataSize is huge.
            //       Perhaps use mmap, or did mina read it entirely 
            //       into memory by this point already?
            //
            val data = new Array[Byte](dataSize)
            
            cmdData.read(data)
            
            if (cmdData.readString(CRNL.length) == CRNL) {
              val cmd = MCommand(cmdArgs,
                                 MEntry(cmdArgs(1),
                                        cmdArgs(2).toLong,
                                        if (expTime != 0 &&
                                            expTime <= SECONDS_IN_30_DAYS)
                                            expTime + nowInSeconds
                                        else
                                            expTime,
                                        dataSize,
                                        data,
                                        (session.getId << 32) + 
                                        (session.getReadMessages & 0xFFFFFFFFL)))
              val res = spec.process(server, cmd, session)
              if (cmd.noReply == false)
                session.write(res)
              GOOD
            } else {
              session.write(MResponseLine("CLIENT_ERROR missing CRNL after data" + CRNL))
              GOOD
            }
          } else {
            totalNeeded
          }
        } else {
          session.write(MResponseLine("CLIENT_ERROR args: " + cmdArgs.mkString(" ") + CRNL))
          GOOD
        }
      }
    ) getOrElse {
      session.write(MResponseLine("ERROR " + cmdName + CRNL)) // Saw an unknown command, but keep
      GOOD                                                    // going and process the next command.
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

      statLine("version", svr.version)

//    statLine("cmd_gets",   String.valueOf(get_cmds))
//    statLine("cmd_sets",   String.valueOf(set_cmds))
//    statLine("get_hits",   String.valueOf(get_hits))
//    statLine("get_misses", String.valueOf(get_misses))

//    statLine("curr_connections",  String.valueOf(curr_conns))
//    statLine("total_connections", String.valueOf(total_conns))
//    statLine("bytes_read",    0.toString)
//    statLine("bytes_written", 0.toString)

      val ctm = System.currentTimeMillis

      statLine("time",   (new java.util.Date()) + " " + ctm.toString)
      statLine("uptime", (ctm - svr.createdAt).toString)

      statLine("curr_items",     svrStats.numEntries.toString)
      statLine("evictions",      svrStats.evictions.toString)
      statLine("bytes",          svrStats.usedMemory.toString)
      statLine("limit_maxbytes", svr.limitMemory.toString)
      statLine("current_bytes",  Runtime.getRuntime.totalMemory.toString)
      statLine("free_bytes",     Runtime.getRuntime.freeMemory.toString)
      statLine("lru_size",       svrStats.lruSize.toString)

//    statLine("pid",           0.toString)
//    statLine("pointer_size",  0.toString)
//    statLine("rusage_user",   "0:0")
//    statLine("rusage_system", "0:0")
//    statLine("threads",       0.toString)
//    statLine("connection_structures", 0.toString)
    }

    sb.append(END)
    sb.toString
  }

/*
    Info on how stats should work, from memcached protocol.txt...
    
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

/**
 * Represents an incoming command or request from a (remote) client.
 */
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

/**
 * Abstract base class that represents a reply or response to processing a MCommand.
 * One MCommand can result in a List of MResponse.
 */
abstract class MResponse {
  def size: Int
  def put(buf: MBufferOut): Unit
}

/**
 * A response of a String of a single line.
 */
case class MResponseLine(line: String) extends MResponse {
  def size = line.length // The line includes CRNL already.

  def put(buf: MBufferOut) {
    buf.put(line.toString.getBytes) // TODO: Need a charset here?
  }
}

/**
 * A response of a String of a single line, followed by a single MEntry data bytes.
 */
case class MResponseLineEntry(line: String, entry: MEntry) extends MResponse {
  def size = line.length + entry.dataSize + CRNL.length // The line includes CRNL already.

  def put(buf: MBufferOut) {
    buf.put(line.toString.getBytes) // TODO: Need a charset here?
    buf.put(entry.data)
    buf.put(CRNLBytes)
  }
}

