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

class MServerRouter(host: String, port: Int) 
  extends MProtocol {
  // Register the protocol messages from the client that we will route...
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

  override def processOneLine(spec: MSpec, clientSession: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int): Int = {
    targetWrite(cmdArr, 0, cmdArrLen) // Forward incoming message from client to downstream target server.
    targetFlush
    if (!noReply(cmdArr, cmdArrLen))  // Forward response(s) from downstream server to the client, if needed.
      processTargetResponse(clientSession)
    GOOD
  }

  override def processTwoLine(spec: MSpec, clientSession: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int,
                              cmdArgs: Seq[String], dataSize: Int): Int = {
    targetWrite(cmdArr, 0, cmdArrLen)
    clientSession.readDirect(dataSize + CRNL.length, targetWriteFunc)
    targetFlush
    if (!noReply(cmdArr, cmdArrLen))
      processTargetResponse(clientSession)
    GOOD
  }

  def processTargetResponse(clientSession: MSession): Unit = {
    var r = clientSession.attachment.asInstanceOf[Response]
    if (r == null) {
        r = new Response(clientSession)
        clientSession.attachment_!(r)
    }
    r.go
  }

  // -----------------------------------------------

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
  class Response(clientSession: MSession) extends MNetworkReader with MSession with MProtocol { 
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = 
      try {
        is.read(buf, offset, length)
      } catch {
        case _ => close; -1
      }

    def connClose: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      this.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
      println(arrayToString(bytes, offset, length)) // TODO: Log these.

    var end = false
    def go  = while (!end) messageRead

    override def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
      if (arrayCompare(VALUEBytes, x) == 0)
        twoLineResponseMarker
      else
        oneLineResponseMarker

    override def processOneLine(spec: MSpec, 
                                targetSession: MSession, 
                                cmdArr: Array[Byte], // Target response bytes.
                                cmdArrLen: Int,
                                cmdLen: Int): Int = {
      clientSession.write(cmdArr, 0, cmdArrLen)
      close // Stop messageRead loop, as we got a one-line response like END, STORED, etc, from the downstream target server.
      GOOD
    }

    override def processTwoLine(spec: MSpec, 
                                targetSession: MSession, 
                                cmdArr: Array[Byte], // Target response bytes.
                                cmdArrLen: Int,
                                cmdLen: Int,
                                cmdArgs: Seq[String],
                                dataSize: Int): Int = {
      clientSession.write(cmdArr, 0, cmdArrLen)
      targetSession.readDirect(dataSize + CRNL.length, clientSessionWriteFunc)
      GOOD
    }

    val clientSessionWriteFunc = (a: Array[Byte], offset: Int, length: Int) => clientSession.write(a, offset, length)
  }

  val oneLineResponseMarker = Some(MSpec("oneLineResponseMarker", (cmd) => { throw new RuntimeException("not expected") }))
  val twoLineResponseMarker = Some(MSpec("twoLineResponseMarker", (cmd) => { throw new RuntimeException("not expected") }))

  protected var s  = new Socket(host, port) // The downstream target server to route to.
  protected var is = s.getInputStream
  protected var os = s.getOutputStream
  protected var bs = new BufferedOutputStream(os)

  def targetWrite(m: String): Unit                                = bs.write(stringToArray(m))
  def targetWrite(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def targetWrite(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def targetWriteFunc = (a: Array[Byte], offset: Int, length: Int) => bs.write(a, offset, length)

  def isTargetClosed = s == null

  def targetClose: Unit = {
    if (s != null)
        s.close
    s  = null
    is = null
    os = null
    bs = null
  }

  def targetFlush: Unit = 
    try {
      bs.flush
    } catch {
      case _ => targetClose
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
