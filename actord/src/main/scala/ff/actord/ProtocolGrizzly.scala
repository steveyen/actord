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

import java.io._
import java.net._
import java.nio._

import com.sun.grizzly._
import com.sun.grizzly.filter._
import com.sun.grizzly.util._

import ff.actord.Util._

/**
 * Sun Grizzly / NIO based acceptor/protocol implementation.
 */
class GAcceptor(server: MServer, protocol: MProtocol, numProcessors: Int, port: Int)
  extends Thread {
  override def run = {
    // See: http://weblogs.java.net/blog/jfarcand/archive/2008/02/writing_a_tcpud_1.html
    //      http://gallemore.blogspot.com/2007/07/using-grizzly-to-read-tcp-packets.html
    //
    val t = new TCPSelectorHandler
    t.setPort(port)
    t.setTcpNoDelay(true)

    val p: ProtocolChain = new DefaultProtocolChain
    p.addFilter(new ReadFilter)
    p.addFilter(new GProtocolFilter(server, protocol)) // TODO: Collapse ReadFilter into our GProtocolFilter.

    val c = new Controller
    c.addSelectorHandler(t)
    c.setProtocolChainInstanceHandler(new ProtocolChainInstanceHandler() {
      def poll: ProtocolChain     = p
      def offer(x: ProtocolChain) = true
    })
    c.start
  }
}

class GProtocolFilter(server: MServer, protocol: MProtocol) extends ProtocolFilter {
  var idCurr = 0L
  def idNext = synchronized {
    idCurr += 1L
    idCurr
  }

  def execute(ctx: Context): Boolean = {
    val bb = Thread.currentThread.asInstanceOf[WorkerThread].getByteBuffer
    if (bb != null) {
        bb.flip
        if (bb.hasRemaining) {
          val k = ctx.getSelectionKey
          var s = k.attachment.asInstanceOf[GSession]
          if (s == null) {
              s = new GSession(server, protocol, k.channel, idNext)
              k.attach(s)
          }
          
          s.incoming(bb, ctx)
        }
        bb.clear
    }
    true
  }

  def postExecute(ctx: Context): Boolean = true
}

class GSession(server: MServer, protocol: MProtocol, s: Closeable, sessionIdent: Long) {
  val MIN_CMD_SIZE = "quit\r\n".length
  
  private var waitingFor    = MIN_CMD_SIZE
  private var availablePrev = 0
  private var available     = 0
  private var readPos       = 0

  private var buf = new Array[Byte](8000)
  
  private var nMessages = 0L

  def bufIndexOf(buf: Array[Byte], offset: Int, n: Int, x: Byte): Int = { // Bounded buf.indexOf(x) method.
    var i = offset
    while (i < n) {
      if (buf(i) == x)
        return i
      i += 1
    }
    -1
  }
  
  def incoming(in: ByteBuffer, ctx: Context): Unit = {
    readPos = 0
    
    val inCount = in.remaining
    
    if (available + inCount > buf.length)
       bufGrow(available + inCount)
    
    in.get(buf, available, inCount)
    
    availablePrev = available
    available     = available + inCount

    if (available >= waitingFor) {
      val indexCR: Int = if (available >= 2 &&
                             available == availablePrev + 1) {
                           if (buf(available - 2) == CR) {
                             available - 2
                           } else
                             -1
                         } else
                           bufIndexOf(buf, 0, available, CR)
      if (indexCR < 0 ||
          indexCR + CRNL.length > available) {
        waitingFor = waitingFor + 1
        if (waitingFor > buf.length)
          bufGrow(waitingFor)
      } else {
        val nLine = indexCR + CRNL.length
        val aLine = new Array[Byte](nLine)

        read(aLine)

        if (aLine(nLine - 2) != CR ||
            aLine(nLine - 1) != NL) {
          s.close
          throw new RuntimeException("missing CRNL")
        } else {
          val bytesNeeded = protocol.process(server, new GSessionContext(ctx), aLine, available)
          if (bytesNeeded == 0) {
            nMessages = nMessages + 1              

            if (available > readPos)
              Array.copy(buf, readPos, buf, 0, available - readPos)
          
            waitingFor    = MIN_CMD_SIZE
            availablePrev = 0
            available     = available - readPos
          } else {
            waitingFor = bytesNeeded
            if (waitingFor > buf.length)
              bufGrow(waitingFor)
          }
        }
      }
    }
  }
  
  def bufGrow(size: Int) = {
    val bufPrev = buf
    buf = new Array[Byte](size)
    Array.copy(bufPrev, 0, buf, 0, bufPrev.length)
  }

  def read: Byte = {
    if (readPos >= available)
      throw new RuntimeException("reading more than available: " + available)
    val b = buf(readPos)
    readPos = readPos + 1
    b
  }
     
  def read(bytes: Array[Byte]): Unit = {
    if (readPos + bytes.length > available)
      throw new RuntimeException("reading more bytes than available: " + available)
    Array.copy(buf, readPos, bytes, 0, bytes.length)
    readPos = readPos + bytes.length
  }
  
  class GSessionContext(ctx: Context) extends MSession {
    def ident: Long = sessionIdent
    def close: Unit = s.close

    def read: Byte                     = GSession.this.read
    def read(bytes: Array[Byte]): Unit = GSession.this.read(bytes)
    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
      ctx.getAsyncQueueWritable.writeToAsyncQueue(ByteBuffer.wrap(bytes, offset, length))

    def numMessages: Long = nMessages
  }
}

