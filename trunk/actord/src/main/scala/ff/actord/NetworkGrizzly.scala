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
class GAcceptor(protocol: MProtocol, numProcessors: Int, port: Int)
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
    p.addFilter(new GProtocolFilter(protocol)) // TODO: Collapse ReadFilter into our GProtocolFilter.

    val c = new Controller
    c.addSelectorHandler(t)
    c.setProtocolChainInstanceHandler(new ProtocolChainInstanceHandler() {
      def poll: ProtocolChain     = p
      def offer(x: ProtocolChain) = true
    })
    c.start
  }
}

class GProtocolFilter(protocol: MProtocol) extends ProtocolFilter {
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
              s = new GSession(protocol, k.channel, idNext)
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

class GSession(protocol: MProtocol, s: Closeable, sessionIdent: Long) 
  extends MSession
     with MNetworkReader {
  override val minMessageLength = "quit\r\n".length
  
  def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int =
    protocol.process(this, cmdArr, cmdLen, available)

  var in: ByteBuffer = _
  var ctx: Context   = _

  def connRead(buf: Array[Byte], offset: Int, length: Int): Int = {
    val r = Math.min(length, in.remaining)
    in.get(buf, offset, r)
    r
  }

  def connClose: Unit = s.close

  def incoming(inIn: ByteBuffer, ctxIn: Context): Unit = {
    in = inIn
    ctx = ctxIn
    while (in.remaining > 0)
      messageRead
  }
  
  def ident: Long = sessionIdent
  def close: Unit = s.close

  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
    ctx.getAsyncQueueWritable.writeToAsyncQueue(ByteBuffer.wrap(bytes, offset, length))
}

