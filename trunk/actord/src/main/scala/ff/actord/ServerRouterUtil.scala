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

/**
 * An MProtocol implementation that routes messages to a remote, downstream target server(s).
 */
trait MServerRouter extends MProtocol {
  def chooseTarget(spec: MSpec, clientSession: MSession, 
                   cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int): MRouterTarget

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
    val target = chooseTarget(spec, clientSession, cmdArr, cmdArrLen, cmdLen)

    target.write(cmdArr, 0, cmdArrLen) // Forward incoming message from client to downstream target server.
    target.writeFlush

    processTargetResponse(target, clientSession, cmdArr, cmdArrLen)
  }

  override def processTwoLine(spec: MSpec, clientSession: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int,
                              cmdArgs: Seq[String], dataSize: Int): Int = {
    val target = chooseTarget(spec, clientSession, cmdArr, cmdArrLen, cmdLen)

    target.write(cmdArr, 0, cmdArrLen)
    clientSession.readDirect(dataSize + CRNL.length, target.writeFunc)
    target.writeFlush

    processTargetResponse(target, clientSession, cmdArr, cmdArrLen)
  }

  // -----------------------------------------------

  def processTargetResponse(target: MRouterTarget, clientSession: MSession, cmdArr: Array[Byte], cmdArrLen: Int): Int = {
    if (!noReply(cmdArr, cmdArrLen)) {
      var m = clientSession.attachment.asInstanceOf[mutable.Map[MRouterTarget, Response]]
      if (m == null) {
          m = new mutable.HashMap[MRouterTarget, Response]
          clientSession.attachment_!(m)
      }
      var r = if (m.contains(target)) 
                m(target) 
              else {
                val rr = new Response(target, clientSession)
                m += (target -> rr)
                rr
              }
      r.go
    }
    GOOD
  }

  def noReply(cmdArr: Array[Byte], cmdArrLen: Int): Boolean = 
    if (cmdArrLen > NOREPLYBytes.length)
      arrayCompare(NOREPLYBytes, 0, NOREPLYBytes.length,
                   cmdArr, cmdArrLen - NOREPLYBytes.length, NOREPLYBytes.length) == 0
    else
      false

  val NOREPLYBytes = stringToArray(" noreply" + CRNL)
  val VALUEBytes   = stringToArray("VALUE ")

  /**
   * Processes the response/reply from the server.
   */
  class Response(target: MRouterTarget, clientSession: MSession) extends MNetworkReader with MSession with MProtocol { 
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = 
      try {
        target.readResponse(buf, offset, length)
      } catch {
        case _ => close; -1
      }

    def connClose: Unit = { end = true } // Overriding the meaning of 'close' to mean stop reading target responses.

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      this.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' to mean stop reading target responses.

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
}

// --------------------------------------------------

trait MRouterTarget {
  def readResponse(buf: Array[Byte], offset: Int, length: Int): Int

  def write(m: String): Unit                               
  def write(a: Array[Byte]): Unit                          
  def write(a: Array[Byte], offset: Int, length: Int): Unit
  def writeFunc: (Array[Byte], Int, Int) => Unit
  def writeFlush: Unit

  def close: Unit
  def isClosed: Boolean
}

