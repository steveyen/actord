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

import java.io._
import java.net._

import ff.actord.Util._
import ff.actord.MProtocol._

object Router {
  def main(args: Array[String]): Unit = (new Router).start(args)
}

class Router {
  def default_port   = "11222"
  def default_target = "127.0.0.1:11211"

  def start(args: Array[String]): Unit =
    start(MainFlag.parseFlags(args, flags, 
                              "router -- routes memcached messages, part of actord project", 
                              MServer.version))
  
  def start(arg: (String, String) => String) {
    val port   = arg("port",   default_port).toInt
    val target = arg("target", default_target)

    val targetParts = target.split(":")
    if (targetParts.length != 2) {
      println("error in target server parameter (-target <host:port>)")
      System.exit(1)
    }

    startAcceptor(targetParts(0), targetParts(1).toInt, port)

    println("listening on port : " + port)
    println("routing to target : " + target)
  }
  
  def startAcceptor(targetHost: String, targetPort: Int, port: Int): Unit = 
    (new SAcceptor(createProtocol(targetHost, targetPort), 1, port)).start

  def createProtocol(targetHost: String, targetPort: Int): MProtocol = 
    new MServerRouter(targetHost, targetPort)

  // ------------------------------------------------------

  def flags = List(
    Flag("target", 
             "-target <host:port>" :: Nil,
             "Route requests to target server <host:port>; default is " + default_target + "."),
    Flag("port", 
             "-p <num>" :: Nil,
             "Listen on port <num>; default is " + default_port + "."),
    Flag("help", 
             "-h" :: "-?" :: "--help" :: Nil,
             "Show the version of the server and a summary of options."),
    Flag("verbose", 
             "-v" :: Nil,
             "Be verbose during processing; print out errors and warnings."),
    Flag("veryVerbose", 
             "-vv" :: Nil,
             "Be even more verbose; for example, also print client requests and responses.")
//  Flag("ipAddr", 
//           "-l <ip_addr>" :: Nil,
//           "Listen on <ip_addr>; default to INDRR_ANY.\n" +
//             "This is an important option to consider for security.\n" +
//             "Binding to an internal or firewalled network interface is suggested."),
//  Flag("noExpire", 
//           "-M" :: Nil,
//           "Instead of expiring items when max memory is reached, throw an error."),
//  Flag("maxConn", 
//           "-c <num>" :: Nil,
//           "Use <num> max simultaneous connections; the default is 1024."),
//  Flag("growCore", 
//           "-r" :: Nil,
//           "Raise the core file size limit to the maximum allowable."),
//  Flag("daemon", 
//           "-d" :: Nil,
//           "Run server as a daemon."),
//  Flag("username", 
//           "-u <username>" :: Nil,
//           "Assume the identity of <username> (only when run as root)."),
//  Flag("lockMem", 
//           "-k" :: Nil,
//           "Lock down all paged memory. This is a somewhat dangerous option with large caches, " +
//             "so consult the docs for configuration suggestions."),
//  Flag("pidFile", 
//           "-P <filename>" :: Nil,
//           "Print pidfile to <filename>, only used under -d option.")
  )
}

class MServerRouter(host: String, port: Int) 
  extends MProtocol {
  protected var s  = new Socket(host, port) // The target server to route to.
  protected var is = s.getInputStream
  protected var os = s.getOutputStream
  protected var bs = new BufferedOutputStream(os)

  def write(m: String): Unit                                = bs.write(stringToArray(m))
  def write(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def write(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def isClosed = s == null

  def close: Unit = {
    if (s != null)
        s.close
    s  = null
    is = null
    os = null
    bs = null
  }

  def flush: Unit = 
    try {
      bs.flush
    } catch {
      case _ => close
    }

  val oneLineRouter = (cmd: MCommand) => { 
    write(cmd.cmdArr, 0, cmd.cmdArrLen)
    flush

//    if (cmd.

//    svr.get(cmd.args).
//        foreach(el => cmd.write(el, false))
    cmd.reply(END)
  }

  val twoLineRouter = (cmd: MCommand) => { 
//    svr.get(cmd.args).
//        foreach(el => cmd.write(el, false))
    cmd.reply(END)
  }

  override def oneLineSpecs = List(
    "get <key>*",
    "gets <key>*",
    "delete <key> [<time>] [noreply]",
    "incr <key> <value> [noreply]",
    "decr <key> <value> [noreply]",
    "stats [<arg>]",
    "flush_all [<delay>] [noreply]",
    "version", 
    "verbosity",
    "quit",
    "range <key_from> <key_to>"
  ).map(s => MSpec(s, oneLineRouter))
           
  override def twoLineSpecs = List( 
    "set <key> <flags> <expTime> <dataSize> [noreply]",
    "add <key> <flags> <expTime> <dataSize> [noreply]",
    "replace <key> <flags> <expTime> <dataSize> [noreply]",
    "append <key> <flags> <expTime> <dataSize> [noreply]",
    "prepend <key> <flags> <expTime> <dataSize> [noreply]",
    "cas <key> <flags> <expTime> <dataSize> <cid_unique> [noreply]",
    "act <key> <flags> <expTime> <dataSize> [noreply]"
  ).map(s => MSpec(s, twoLineRouter))

  /**
   * Processes the response/reply from the server.
   */
  class Response(protocol: MProtocol) extends MNetworkReader with MSession { 
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = 
      try {
        is.read(buf, offset, length)
      } catch {
        case _ => close; -1
      }

    def connClose: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      protocol.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
      println(arrayToString(bytes, offset, length)) // TODO: Log these.

    var end = false
    def go  = while (!end) messageRead
  }

/*
  val responseProtocol = new MProtocol {
    override def twoLineSpecs = List(
      new MSpec("VALUE <key> <flags> <dataSize> [cid]", (cmd) => { xs = cmd.entry :: xs }) {
        override def casParse(cmdArgs: Seq[String]) = itemToLong(cmdArgs, pos_cas, 0L)
      })
    override def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
      super.findSpec(x, xLen, lookup).
            orElse(oneLineResponse)
  }
*/

  val oneLineResponse = Some(
    MSpec("N/A", 
          (cmd) => { 
//            cmd.reply(cmd.cmdArr) 
    })
  )

  def responseValues: Iterator[MEntry] = {
    var xs: List[MEntry] = Nil
    new Response(new MProtocol {
      override def oneLineSpecs = List(MSpec("END", (cmd) => { cmd.session.close }))
      override def twoLineSpecs = List(
        new MSpec("VALUE <key> <flags> <dataSize> [cid]", (cmd) => { xs = cmd.entry :: xs }) {
          override def casParse(cmdArgs: Seq[String]) = itemToLong(cmdArgs, pos_cas, 0L)
        })
    }).go
    xs.elements
  }

  def response(good: String, fail: String, async: Boolean): Boolean = {
    var result = true
    if (!async) 
      new Response(new MProtocol {
        override def oneLineSpecs = List(
          MSpec(good, (cmd) => { result = true;  cmd.session.close }),
          MSpec(fail, (cmd) => { result = false; cmd.session.close }))
      }).go
    result
  }

  val getBytes      = stringToArray("get ")
  val getsBytes     = stringToArray("gets ")
  val setBytes      = stringToArray("set ")
  val deleteBytes   = stringToArray("delete ")
  val incrBytes     = stringToArray("incr ")
  val decrBytes     = stringToArray("decr ")
  val addBytes      = stringToArray("add ")
  val replaceBytes  = stringToArray("replace ")
  val appendBytes   = stringToArray("append ")
  val prependBytes  = stringToArray("prepend ")
  val casBytes      = stringToArray("cas ")
  val rangeBytes    = stringToArray("range ")
  val actBytes      = stringToArray("act ")
  val flushAllBytes = stringToArray("flush_all ")
  val noreplyBytes  = stringToArray(" noreply")

  def get(keys: Seq[String]): Iterator[MEntry] = { // We send a gets, instead of a get, in order
    write(getsBytes)                               // to receive full CAS information.
    write(keys.mkString(" "))
    write(CRNLBytes)
    flush
    responseValues
  }

  def writeNoReplyFlag(async: Boolean): Unit =
    if (async)
      write(noreplyBytes)

  def set(el: MEntry, async: Boolean): Boolean = {
    write(setBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  def delete(key: String, time: Long, async: Boolean): Boolean = {
    write(deleteBytes)
    write(key + " " + time)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    flush
    response("DELETED", "NOT_FOUND", async)
  }

  /**
   * A transport protocol can convert incoming incr/decr messages to delta calls.
   */
  def delta(key: String, mod: Long, async: Boolean): Long = {
    if (mod > 0L) 
      write(incrBytes)
    else 
      write(decrBytes)
    write(key + " " + Math.abs(mod))
    writeNoReplyFlag(async)
    write(CRNLBytes)
    flush

    var result = -1L
    if (!async) 
      new Response(new MProtocol {
        override def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
          Some(MSpec("UNUSED", (cmd) => { 
            val s = arrayToString(x, 0, xLen)
            if (s != "NOT_FOUND")
              result = parseLong(s, -1L)
            cmd.session.close 
          }))
      }).go
    result
  }
    
  /**
   * For add or replace.
   */
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean): Boolean = {
    if (isAdd) 
      write(addBytes)
    else
      write(replaceBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * A transport protocol can convert incoming append/prepend messages to xpend calls.
   */
  def xpend(el: MEntry, append: Boolean, async: Boolean): Boolean = {
    if (append) 
      write(appendBytes)
    else
      write(prependBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * For CAS mutation.
   */  
  def checkAndSet(el: MEntry, cid: Long, async: Boolean): String = {
    write(casBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + " " + cid)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush

    var result = ""
    if (!async) 
      new Response(new MProtocol {
        override def oneLineSpecs = List(
          MSpec("STORED",    (cmd) => { result = "STORED";    cmd.session.close }),
          MSpec("EXISTS",    (cmd) => { result = "EXISTS";    cmd.session.close }),
          MSpec("NOT_FOUND", (cmd) => { result = "NOT_FOUND"; cmd.session.close }))
      }).go
    result
  }

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String] = Nil.elements
  
  def flushAll(expTime: Long): Unit = {
    write(flushAllBytes)
    write(expTime.toString)
    write(CRNLBytes)
    flush
    response("OK", "NOT_OK", false)
  }
  
  def stats: MServerStats = null

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    write(rangeBytes)
    write(keyFrom)
    write(SPACEBytes)
    write(keyTo)
    write(CRNLBytes)
    flush
    responseValues
  }

  def act(el: MEntry, async: Boolean): Iterator[MEntry] = {
    write(actBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    responseValues
  }
}

class RouterProtocol(targetHost: String, targetPort: Int) extends MProtocolServer(null) {
  // unfinished: 
  //  also can do a tee here, replicator, two-level cache
  /*
    proxyServer        = ServerProxy(Client(targetHost, targetPort))
    loggingProxyServer = LoggingProxy(ServerProxy(Client(targetHost, targetPort)))
    replicatingProxy   = ServerProxy(Client(targetHost, targetPort),
                                     Client(targetHost, targetPort))

    the localCache is the hard one...
      need individual specs
        read-thru get spec
        write-thru set spec

    localCache = ReadThruProxy(Client(targetHost, targetPort)))
    localCache = WriteThruProxy(Client(targetHost, targetPort)))

    Option[localServer]
    List[remoteServer]
    rules on how to choose between and process commands

    if sync-write-thru and set
      set into remote

    if localServer
      try local first
      if miss and sync-read-thru
        get from remote and add to local
        return hit or miss

   router matrix          synch            or async
     write-thru-to-remote before or after  before or after
     read-thru-to-remote  before or after  before or after

   syncLevel is 0-sync, 1-sync, Quorum-sync, N-sync, All-sync

   // By unified, we mean this pseudocode handles read and write ops.
   //
   // The primary might be the local store.
   // The replicas might be 1 or more remote stores.
   //
   def unifiedOp(...) = { 
     var repNBefore = _
     var repNAfter  = _

     if (startReplicas == before)
       repNBefore = startReplicas(syncLevel)

     var pri = doPrimary

     if (startReplicas == after &&
         pri & repNBefore are not satisfied)
       repNAfter = startReplicas(syncLevel)

     wait until (pri & repNBefore & repNAfter are satisfied) or timeout
   }

   MServer is a server interface
   MMainServer implements MServer, is local in-mem cache/server
   MServerProxy implements MServer, forwards msgs to a single remote server at host:port
   MServerRouter forwards to 1 or more MServer (which might just be proxies)

  */
}
