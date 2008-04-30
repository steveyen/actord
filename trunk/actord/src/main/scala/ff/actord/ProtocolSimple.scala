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

import ff.actord.Util._

class SAcceptor(server: MServer, protocol: MProtocol, numProcessors: Int, port: Int) 
  extends Thread {
  override def run = {
    var idGen = 0L
    val ss    = new ServerSocket(port)
    
    while (true) {
      (new SSession(server, protocol, ss.accept, idGen)).start
      idGen = idGen + 1L
    }
  }
}

class SSession(server: MServer, protocol: MProtocol, s: Socket, id: Long) 
  extends Thread 
     with MBufferIn
     with MBufferOut
     with MSession {
  s.setTcpNoDelay(true)
  
  val MIN_CMD_SIZE = "quit\r\n".length
  
  private var waitingFor = MIN_CMD_SIZE
  private var available  = 0
  private var readPos    = 0

  private var buf = new Array[Byte](waitingFor)
  private val is = s.getInputStream
  private val os = s.getOutputStream
  
  private var numMessages = 0L

  override def run = {
    while (s.isClosed == false) {
      readPos = 0
      
      val lastRead = is.read(buf, available, buf.length - available)
      if (lastRead <= -1)
        s.close
        
      available = available + lastRead
      if (available > buf.length) {
        s.close
        throw new RuntimeException("available larger than buf somehow")
      }

      if (available >= waitingFor) {
        val indexCR = buf.indexOf(CR)
        if (indexCR < 0 ||
            indexCR + CRNL.length > available) {
          waitingFor = waitingFor + 1
          if (waitingFor > buf.length)
            bufGrow(waitingFor)
        } else {
          val line = readString(indexCR + CRNL.length)
          if (line.endsWith(CRNL) == false) {
            s.close
            throw new RuntimeException("missing CRNL")
          } else {
            val bytesNeeded = protocol.process(server, this, line, this, available)
            if (bytesNeeded == 0) {
              numMessages = numMessages + 1              

              if (available > readPos)
                Array.copy(buf, readPos, buf, 0, available - readPos)
            
              waitingFor = MIN_CMD_SIZE
              available  = available - readPos
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
     
  def read(bytes: Array[Byte]): Unit = {
    if (readPos + bytes.length > available)
      throw new RuntimeException("reading more bytes than available: " + available)

    Array.copy(buf, readPos, bytes, 0, bytes.length)
    readPos = readPos + bytes.length
  }
  
  def readString(num: Int): String = {
    if (readPos + num > available)
      throw new RuntimeException("reading more string than available: " + available)
      
    val r = new String(buf, readPos, num, "US-ASCII")
    readPos = readPos + num
    r
  } 
  
  def close: Unit = s.close

  def write(res: MResponse): Unit = res.put(this)
  
  def getReadMessages: Long = numMessages

  def put(bytes: Array[Byte]): Unit = os.write(bytes)
}

