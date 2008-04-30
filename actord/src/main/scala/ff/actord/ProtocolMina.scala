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

import java.net._
import java.nio.charset._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

class MMinaHandler(server: MServer) extends IoHandlerAdapter {
  val log = LoggerFactory.getLogger(getClass)
  
  override def exceptionCaught(session: IoSession, cause: Throwable) = {
    log.warn("unexpected exception: ", cause)
    session.close
  }
  
  override def messageReceived(session: IoSession, message: Object): Unit = {
    // log.info("received message")
  }

  override def sessionOpened(sess: IoSession): Unit = {
    sess.getConfig match {
      case ssc: SocketSessionConfig => ssc.setTcpNoDelay(true)
      case _ =>
    }
  }
}

// -------------------------------------------------------

/**
 * Protocol is defined at:
 * http://code.sixapart.com/svn/memcached/trunk/server/doc/protocol.txt
 *
 * TODO: See if we can use something lower-level, like 
 *       CummulativeProtocolDecoder, for more performance.
 */
class MMinaDecoder(server: MServer, protocol: MProtocol) extends MessageDecoder {
  val charsetDecoder     = Charset.forName("US-ASCII").newDecoder
  val MIN_CMD_SIZE       = "quit\r\n".length
  val WAITING_FOR        = new AttributeKey(getClass, "waiting_for")  
  val STATS              = new AttributeKey(getClass, "stats")  
  
  def decodable(session: IoSession, in: IoBuffer): MessageDecoderResult = {
    val waitingFor = session.getAttribute(WAITING_FOR, ZERO).asInstanceOf[java.lang.Integer].intValue
    if (waitingFor == 0) {
      if (in.remaining >= MIN_CMD_SIZE)
        MessageDecoderResult.OK
      else
        MessageDecoderResult.NEED_DATA
    }
    
    if (waitingFor <= in.remaining)
      MessageDecoderResult.OK
    else  
      MessageDecoderResult.NEED_DATA
  }
  
  def decode(session: IoSession, in: IoBuffer, out: ProtocolDecoderOutput): MessageDecoderResult = {
    val remaining = in.remaining
    val waitingFor = session.getAttribute(WAITING_FOR, ZERO).asInstanceOf[java.lang.Integer].intValue
    if (waitingFor > 0) {
      if (waitingFor <= remaining)
        session.setAttribute(WAITING_FOR, ZERO)
      else
        return MessageDecoderResult.NEED_DATA
    }
    
    val indexCR = in.indexOf(CR)
    if (indexCR < 0)
        return MessageDecoderResult.NEED_DATA

    if (indexCR + CRNL.length > remaining) 
        return MessageDecoderResult.NEED_DATA

    val wrap = WrapIoBufferIn(in)
    val line = wrap.readString(indexCR + CRNL.length)
    if (line.endsWith(CRNL) == false)
        return MessageDecoderResult.NOT_OK // TODO: Need to close session here?
        
    val bytesNeeded = protocol.process(server, WrapIoSession(session), line, wrap, remaining)
    if (bytesNeeded == 0) {
      MessageDecoderResult.OK
    } else {
      session.setAttribute(WAITING_FOR, new java.lang.Integer(bytesNeeded))
      in.rewind
      MessageDecoderResult.NEED_DATA
    }
  }
  
  def finishDecode(session: IoSession, out: ProtocolDecoderOutput) = {
    // TODO: Do we need to do something here?  Or just drop the message on the floor?
  }
  
  case class WrapIoBufferIn(buf: IoBuffer) extends MBufferIn {
    def read(bytes: Array[Byte]): Unit = buf.get(bytes)
    def readString(num: Int): String = {
      val a = new Array[Byte](num)
      buf.get(a, 0, num)
      new String(a, 0, num, "US-ASCII")
    }
  }
  
  case class WrapIoSession(sess: IoSession) extends MSession {
    def getId: Long               = sess.getId
    def close: Unit               = sess.close
    def write(r: MResponse): Unit = sess.write(r)
  
    def getReadMessages: Long = sess.getReadMessages
  }
}

// -------------------------------------------------------

class MMinaEncoder(server: MServer, protocol: MProtocol) extends MessageEncoder[MResponse] {
  case class WrapIoBufferOut(buf: IoBuffer) extends MBufferOut {
    def put(bytes: Array[Byte]): Unit = buf.put(bytes)
  }
  
  def encode(session: IoSession, message: MResponse, out: ProtocolEncoderOutput) {
    val buf = IoBuffer.allocate(message.size)

    message.put(WrapIoBufferOut(buf))

    buf.flip
    out.write(buf)
  }
}

