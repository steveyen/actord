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

class SAcceptor(protocol: MProtocol, numProcessors: Int, port: Int) 
  extends Thread {
  override def run = {
    var id = 0L
    val ss = new ServerSocket(port)

    while (true) {
      acceptSocket(ss.accept, id)
      id += 1L
    }
  }

  def acceptSocket(s: Socket, id: Long): Unit = {
    s.setTcpNoDelay(true)
    (new SSession(protocol, s, id)).start
  }
}

class SSession(protocol: MProtocol, s: Socket, sessionIdent: Long) 
  extends Thread 
     with MSession {
  val MIN_CMD_SIZE = "quit\r\n".length
  
  private var waitingFor    = MIN_CMD_SIZE
  private var availablePrev = 0
  private var available     = 0
  private var readPos       = 0

  private var buf = new Array[Byte](8000)
  private val is  = s.getInputStream
  private val os  = s.getOutputStream
  private val bos = new BufferedOutputStream(os, 4000)
  
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

  override def run = {
    while (s.isClosed == false) {
      readPos = 0
      
      val lastRead = is.read(buf, available, buf.length - available)
      if (lastRead <= -1)
        s.close
      
      availablePrev = available
      available += lastRead
      if (available > buf.length) {
        s.close
        throw new RuntimeException("available larger than buf somehow")
      }

      if (available >= waitingFor) {
        val indexCR: Int = if (available >= 2 &&                 // Optimization to avoid scanning 
                               available == availablePrev + 1) { // the entire buf again for a CR.
                             if (buf(available - 2) == CR) { 
                               available - 2                 
                             } else
                               -1
                           } else
                             bufIndexOf(buf, 0, available, CR)
        if (indexCR < 0 ||
            indexCR + CRNL.length > available) {
          waitingFor += 1
          if (waitingFor > buf.length)
            bufGrow(waitingFor)
        } else {
          val cmdLen = indexCR + CRNL.length

          if (buf(cmdLen - 2) != CR ||
              buf(cmdLen - 1) != NL) {
            s.close
            throw new RuntimeException("missing CRNL")
          } else {
            readPos = cmdLen

            val bytesNeeded = protocol.process(this, buf, cmdLen, available)
            if (bytesNeeded == 0) {
              if (available > readPos)
                Array.copy(buf, readPos, buf, 0, available - readPos)
              else
                bos.flush // Only force flush when there's no more input.
            
              available -= readPos
              availablePrev = 0
              waitingFor = MIN_CMD_SIZE

              nMessages += 1L
            } else {
              waitingFor = bytesNeeded
              if (waitingFor > buf.length)
                bufGrow(waitingFor)
            }
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
    readPos += 1
    b
  }
     
  def read(bytes: Array[Byte]): Unit = {
    if (readPos + bytes.length > available)
      throw new RuntimeException("reading more bytes than available: " + available)
    Array.copy(buf, readPos, bytes, 0, bytes.length)
    readPos += bytes.length
  }
  
  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = bos.write(bytes, offset, length)

  def ident: Long = sessionIdent  
  def close: Unit = s.close

  def numMessages: Long = nMessages
}

