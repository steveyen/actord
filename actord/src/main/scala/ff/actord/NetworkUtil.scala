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

trait MNetworkReader {
  def connRead(buf: Array[Byte], offset: Int, length: Int): Int
  def connClose: Unit

  def minMessageLength: Int = 0

  private var waitingFor    = minMessageLength
  private var availablePrev = 0
  private var available     = 0
  private var readPos       = 0

  private var buf = new Array[Byte](8000)
  
  private var nMessages = 0L

  def numMessages: Long = nMessages

  /**
   * Called by messageRead() when there's a full message to process.  Returns 0 on success,
   * or the number of bytes that messageRead needs to receive 
   * before calling messageProcess again.
   */
  def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int

  /**
   * Meant to be called by a driver loop to continually process incoming bytes.
   * Invokes connRead() to get the input bytes, and when we think there's a 
   * complete message ready for processing, invokes messageProcess().  
   * Invokes connClose() when there's an error.
   */
  def messageRead: Boolean = {
    readPos = 0
      
    val lastRead = connRead(buf, available, buf.length - available)
    if (lastRead <= -1)
      connClose
     
    availablePrev = available
    available += lastRead
    if (available > buf.length) {
      connClose
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
                           bufIndexOf(available, CR)
      if (indexCR < 0 ||
          indexCR + CRNL.length > available) {
        waitingFor += 1
        bufEnsureSize(waitingFor)
      } else {
        val cmdLen = indexCR + CRNL.length

        if (buf(cmdLen - 2) != CR ||
            buf(cmdLen - 1) != NL) {
          connClose
          throw new RuntimeException("missing CRNL")
        } else {
          readPos = cmdLen

          val bytesNeeded = messageProcess(buf, cmdLen, available)
          if (bytesNeeded == 0) {
            if (available > readPos)
              Array.copy(buf, readPos, buf, 0, available - readPos)
            
            available -= readPos
            availablePrev = 0
            waitingFor = minMessageLength

            nMessages += 1L

            return true // We've successfully read and processed a message.
          } else {
            waitingFor = bytesNeeded
            bufEnsureSize(waitingFor)
          }
        }
      }
    }

    false // Returns false if we haven't successfully read and processed a n
  }       // message, and the caller should invoke us again.
  
  private def bufIndexOf(n: Int, x: Byte): Int = { // Bounded buf.indexOf(x) method.
    val b = buf
    var i = 0 
    while (i < n) {
      if (b(i) == x)
        return i
      i += 1
    }
    -1
  }

  def bufEnsureSize(size: Int): Unit = 
    if (size > buf.length) {
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
}

