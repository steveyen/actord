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

// ------------------------------------------------------

class MServerRouter(host: String, port: Int) 
  extends MProtocol {
  // These are the one and two-lined protocol messages from the client that we will route...
  //
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
  ).map(s => MSpec(s, (cmd) => { throw new RuntimeException("unexpected") }))
           
  override def twoLineSpecs = List( 
    "set <key> <flags> <expTime> <dataSize> [noreply]",
    "add <key> <flags> <expTime> <dataSize> [noreply]",
    "replace <key> <flags> <expTime> <dataSize> [noreply]",
    "append <key> <flags> <expTime> <dataSize> [noreply]",
    "prepend <key> <flags> <expTime> <dataSize> [noreply]",
    "cas <key> <flags> <expTime> <dataSize> <cid_unique> [noreply]",
    "act <key> <flags> <expTime> <dataSize> [noreply]"
  ).map(s => MSpec(s, (cmd) => { throw new RuntimeException("unexpected") }))

  // -----------------------------------------------

  override def processOneLine(spec: MSpec, session: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int): Int = {
    write(cmdArr, 0, cmdArrLen)      // Forward incoming message from client to downstream server.
    flush
    if (!noReply(cmdArr, cmdArrLen)) // Forward to the client any responses, if needed.
      processResponse(session).go
    GOOD
  }

  override def processTwoLine(spec: MSpec, session: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int,
                              cmdArgs: Seq[String], dataSize: Int): Int = {
    write(cmdArr, 0, cmdArrLen)
    session.readDirect(dataSize + CRNL.length, writeFunc)
    flush                      
    if (!noReply(cmdArr, cmdArrLen))
      processResponse(session).go
    GOOD
  }

  // -----------------------------------------------

  def processResponse(clientSession: MSession) = // Forward any responses back to the client.
    new Response(new MProtocol {
      override def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
        if (arrayCompare(VALUEBytes, x) == 0)
          twoLineResponseMarker
        else
          oneLineResponseMarker

      override def processOneLine(spec: MSpec, 
                                  session: MSession, 
                                  cmdArr: Array[Byte],
                                  cmdArrLen: Int,
                                  cmdLen: Int): Int = {
        clientSession.write(cmdArr, 0, cmdArrLen)
        close // Response.this.close -- stop reading responses, since we got our one-line response like END, STORED, etc.
        GOOD
      }

      override def processTwoLine(spec: MSpec, 
                                  session: MSession, 
                                  cmdArr: Array[Byte],
                                  cmdArrLen: Int,
                                  cmdLen: Int,
                                  cmdArgs: Seq[String],
                                  dataSize: Int): Int = {
        clientSession.write(cmdArr, 0, cmdArrLen)
        session.readDirect(dataSize + CRNL.length, clientSessionWriteFunc)
        GOOD
      }

      val clientSessionWriteFunc = (a: Array[Byte], offset: Int, length: Int) => clientSession.write(a, offset, length)
    })

  val oneLineResponseMarker = Some(MSpec("oneLineResponseMarker", (cmd) => { throw new RuntimeException("not expected") }))
  val twoLineResponseMarker = Some(MSpec("twoLineResponseMarker", (cmd) => { throw new RuntimeException("not expected") }))

  val VALUEBytes   = stringToArray("VALUE ")
  val NOREPLYBytes = stringToArray(" noreply" + CRNL)

  def noReply(cmdArr: Array[Byte], cmdArrLen: Int): Boolean = 
    if (cmdArrLen > NOREPLYBytes.length)
      arrayCompare(NOREPLYBytes, 0, NOREPLYBytes.length,
                   cmdArr, cmdArrLen - NOREPLYBytes.length, NOREPLYBytes.length) == 0
    else
      false

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

  protected var s  = new Socket(host, port) // The target server to route to.
  protected var is = s.getInputStream
  protected var os = s.getOutputStream
  protected var bs = new BufferedOutputStream(os)

  def write(m: String): Unit                                = bs.write(stringToArray(m))
  def write(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def write(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def writeFunc = (a: Array[Byte], offset: Int, length: Int) => bs.write(a, offset, length)

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
