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

import ff.actord.Util._

class UAcceptor(protocol: MProtocol, numProcessors: Int, port: Int) 
  extends Thread
     with MSession 
     with MNetworkReader {
  val UDP_FRAME_HEADER_SIZE = 8
  val UDP_FRAME_BODY_SIZE   = 1400
  val UDP_FRAME_SIZE        = UDP_FRAME_HEADER_SIZE + UDP_FRAME_BODY_SIZE

  val bufIn     = new Array[Byte](65536)
  var bufOut    = new Array[Byte](65536)
  var bufInPos  = 0
  var bufOutPos = 0

  val s    = new DatagramSocket(port)
  val pIn  = new DatagramPacket(bufIn,  bufIn.length)
  val pOut = new DatagramPacket(bufOut, bufOut.length)

  var msgNum = 0L
  var reqId  = 0
  var seqId  = 0

  override def run =
    while (true) {
      s.receive(pIn)
      if (pIn.getLength >= UDP_FRAME_HEADER_SIZE) {
        // From memcached protocol.txt: The frame header is 
        // 8 bytes long, as follows (all values are 16-bit integers 
        // in network byte order, high byte first):
        // 
        // 0-1 Request ID
        // 2-3 Sequence number
        // 4-5 Total number of datagrams in this message
        // 6-7 Reserved for future use; must be 0
        // 
        // Drop any multi-packet request for now.
        //
        if (bufIn(4) == 0 && bufIn(5) == 1) {
          reqId     = (bufIn(0) * 256) + bufIn(1)
          seqId     = 0
          bufOutPos = UDP_FRAME_HEADER_SIZE
          bufInPos  = UDP_FRAME_HEADER_SIZE

          messageRead
        } else {
          // TODO: send error reply:
          // "SERVER_ERROR multi-packet request not supported"
        }
      }
    }

  override val minMessageLength = "quit\r\n".length
  
  def connRead(buf: Array[Byte], offset: Int, length: Int): Int = {
    val n = Math.min(length, pIn.getLength - bufInPos)
    if (n > 0) {
      Array.copy(bufIn, bufInPos, buf, offset, n)
      bufInPos += n
      n
    } else
      -1
  }

  def connClose: Unit = { /* NO-OP. */ }

  def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = {
    val bytesNeeded = protocol.process(this, cmdArr, cmdLen, available)
    if (bytesNeeded == 0)
      flush
    bytesNeeded
  }

  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    if (bufOutPos + length > bufOut.length) { // Ensure size.
      var bufOutPrev = bufOut
      bufOut = new Array[Byte]((bufOutPos + length) * 2)
      Array.copy(bufOutPrev, 0, bufOut, 0, bufOutPrev.length)
    }
    Array.copy(bytes, offset, bufOut, bufOutPos, length)
    bufOutPos += length
  }

  def flush: Unit = {
    if (bufOutPos > UDP_FRAME_HEADER_SIZE) {
      val numPackets = ((bufOutPos - UDP_FRAME_HEADER_SIZE) / UDP_FRAME_BODY_SIZE) + 1

      for (i <- 0 until numPackets) {   // We're destructive as we move thru bufOut.
        val f = i * UDP_FRAME_BODY_SIZE // The start of the packet, at its frame header.

        bufOut(f + 0) = bufIn(0)
        bufOut(f + 1) = bufIn(1)
        bufOut(f + 2) = (i / 256).toByte
        bufOut(f + 3) = (i % 256).toByte
        bufOut(f + 4) = (numPackets / 256).toByte
        bufOut(f + 5) = (numPackets % 256).toByte
        bufOut(f + 6) = 0
        bufOut(f + 7) = 0

        val packetSize = Math.min(f + UDP_FRAME_SIZE, bufOutPos) - f

        pOut.setAddress(pIn.getAddress)
        pOut.setPort(pIn.getPort)
        pOut.setData(bufOut, f, packetSize)

        s.send(pOut)
      }
    }
    bufOutPos = UDP_FRAME_HEADER_SIZE
  }

  def ident: Long = msgNum
  def close: Unit = { /* NO-OP. */ }
}

