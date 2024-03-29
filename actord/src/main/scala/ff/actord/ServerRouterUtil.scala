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
  override def specs = List(
    "get <key>*",
    "gets <key>*",
    "delete <key> [<time>] [noreply]",
    "set <key> <flags> <expTime> <dataSize> [noreply]",
    "add <key> <flags> <expTime> <dataSize> [noreply]",
    "replace <key> <flags> <expTime> <dataSize> [noreply]",
    "append <key> <flags> <expTime> <dataSize> [noreply]",
    "prepend <key> <flags> <expTime> <dataSize> [noreply]",
    "cas <key> <flags> <expTime> <dataSize> <cid_unique> [noreply]",
    "incr <key> <value> [noreply]",
    "decr <key> <value> [noreply]",
    "stats [<arg>]",
    "flush_all [<delay>] [noreply]",
    "version", 
    "verbosity",
    "quit",
    "range <key_from> <key_to>",
    "act <key> <flags> <expTime> <dataSize> [noreply]"
  ).map(s => MSpec(s, (cmd) => { throw new RuntimeException("unexpected") }))

  // -----------------------------------------------

  override def processCommand(spec: MSpec, clientSession: MSession, 
                              cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int, dataSize: Int): Int = 
    if (arrayCompare(cmdArr, cmdLen, QUITBytes, QUITBytes.length) != 0) {
      routeClientMessage(spec, clientSession, cmdArr, cmdArrLen, cmdLen, dataSize)
    } else {
      clientSession.close
      GOOD
    }

  // -----------------------------------------------

  def routeClientMessage(spec: MSpec, clientSession: MSession, 
                         cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int, dataSize: Int): Int = {
    val target = chooseTarget(spec, clientSession, cmdArr, cmdArrLen, cmdLen)

    // The big synchronized block is because only one client may 
    // forward to a downstream target server at a time.
    //
    try {
      target.synchronized {
        target.write(cmdArr, 0, cmdArrLen)
        if (dataSize >= 0)
          clientSession.readDirect(dataSize + CRNL.length, target.writeFunc)
        target.writeFlush
        processTargetResponse(target, clientSession, cmdArr, cmdArrLen)
      }
    } catch {
      case ex @ _ => clientSession.close; throw ex // Not closing target, as it still might be ok.
    }

    GOOD
  }

  // -----------------------------------------------

  def processTargetResponse(target: MRouterTarget, clientSession: MSession, cmdArr: Array[Byte], cmdArrLen: Int): Unit = {
    if (!arrayEndsWith(cmdArr, cmdArrLen, NOREPLYBytes)) {
      var m = clientSession.attachment.asInstanceOf[java.util.HashMap[MRouterTarget, MRouterTargetResponse]]
      if (m == null) {
          m = new java.util.HashMap[MRouterTarget, MRouterTargetResponse]
          clientSession.attachment_!(m)
      }
      var r = m.get(target) // NOTE: Avoiding the idiomatic scala Map/getOrElse/closure/Option approach
      if (r == null) {      //       in order to prevent throwaway garbage in the core router loop.
          r = new MRouterTargetResponse(target, clientSession)
          m.put(target, r)
      }
      r.go
    }
  }

  val NOREPLYBytes = stringToArray(" noreply" + CRNL)
  val QUITBytes    = stringToArray("quit")
}

/**
 * Processes the response/reply from the downstream target server.
 */
class MRouterTargetResponse(target: MRouterTarget, clientSession: MSession) 
  extends MNetworkReader with MSession {
  protected var goEnd = false

  def go = {
    goEnd = false
    while (!goEnd) 
      messageRead
  }

  val VALUEBytes = stringToArray("VALUE")
  val STATBytes  = stringToArray("STAT")

  // MNetworkReader part...
  //
  def connRead(buf: Array[Byte], offset: Int, length: Int): Int = 
    try {
      target.readResponse(buf, offset, length)
    } catch {
      case ex @ _ => close; target.close; clientSession.close; throw ex
    }

  def connClose: Unit = { goEnd = true } // Overriding the meaning of 'close' to mean stop reading target responses.

  def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
    protocol.process(this, cmdArr, cmdLen, available)

  // MSession part...
  //
  def ident: Long = 0L
  def close: Unit = { goEnd = true } // Overriding the meaning of 'close' to mean stop reading target responses.

  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
    println(arrayToString(bytes, offset, length)) // TODO: Log these.

  // MProtocol part...
  //
  val protocol = new MProtocol() {
    override def findSpec(x: Array[Byte], xLen: Int): MSpec = 
      if (arrayCompare(x, xLen, VALUEBytes, VALUEBytes.length) != 0)
        specReplyOneLine
      else
        specReplyTwoLine

    val specReplyOneLine = MSpec("UNUSED", 
                                 (cmd) => { throw new RuntimeException("cmd not expected to be used") })

    val specReplyTwoLine = MSpec("VALUE <key> <flags> <dataSize> [cid]", 
                                 (cmd) => { throw new RuntimeException("cmd not expected to be used") })

    override def processCommand(spec: MSpec, 
                                targetSession: MSession, 
                                cmdArr: Array[Byte], // Target response bytes.
                                cmdArrLen: Int,
                                cmdLen: Int,
                                dataSize: Int): Int = {
      clientSession.write(cmdArr, 0, cmdArrLen)

      if (dataSize >= 0)
        targetSession.readDirect(dataSize + CRNL.length, clientSessionWriteFunc)
      else {
        // We got a full response if the downstream target server responded
        // with a no-data message, such as END, ERROR, STORED, NOT_STORED, etc,
        // except for STAT.
        //
        if (arrayCompare(cmdArr, cmdLen, STATBytes, STATBytes.length) != 0)
          close // Stop the go/messageRead loop.
      }

      GOOD
    }

    val clientSessionWriteFunc = (a: Array[Byte], offset: Int, length: Int) => clientSession.write(a, offset, length)
  }
}

// --------------------------------------------------

trait MRouterTarget {
  def readResponse(buf: Array[Byte], offset: Int, length: Int): Int
  def write(a: Array[Byte], offset: Int, length: Int): Unit
  def writeFunc: (Array[Byte], Int, Int) => Unit
  def writeFlush: Unit
  def close: Unit
  def isClosed: Boolean
}

