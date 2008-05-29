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
