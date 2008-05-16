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
     with MSession 
     with MNetworkReader {
  override val minMessageLength = "quit\r\n".length
  
  private val is  = s.getInputStream
  private val os  = s.getOutputStream
  private val bos = new BufferedOutputStream(os, 4000)

  def connReadProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = {
    val bytesNeeded = protocol.process(this, cmdArr, cmdLen, available)
    if (bytesNeeded == 0)
      bos.flush
    bytesNeeded
  }

  def connRead(buf: Array[Byte], offset: Int, length: Int): Int = is.read(buf, offset, length)
  def connContinue: Boolean = s.isClosed == false
  def connClose: Unit = s.close

  override def run = {
    while (s.isClosed == false)
      readMore
  }

  def write(bytes: Array[Byte], offset: Int, length: Int): Unit = bos.write(bytes, offset, length)

  def ident: Long = sessionIdent  
  def close: Unit = s.close
}

