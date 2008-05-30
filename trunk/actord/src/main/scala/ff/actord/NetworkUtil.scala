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

/**
 * Low level reader of the memcached protocol, chopping up incoming bytes
 * into messages by looking for CRNL markers.
 */
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
   * Called by messageRead() when there's incoming bytes ready for processing,
   * or when a CRNL is seen.  The messageProcess implementation should
   * return the number of bytes (> 0) that messageRead needs to receive 
   * before calling messageProcess again.  Or, messageProcess should return zero 
   * if one message was successfuly processed from the incoming bytes available.
   * The bufLen is the number of bytes to the first CRNL (inclusive of the CRNL).
   * The available is the number of bytes (sometimes > bufLen) already received.
   */
  def messageProcess(buf: Array[Byte], bufLen: Int, available: Int): Int

  /**
   * Meant to be called by an external driver loop to continually process incoming bytes.
   * Invokes connRead() to get more input bytes, and when it think there's a 
   * complete message ready for processing, invokes messageProcess().  
   * Invokes connClose() when there's an error.
   */
  def messageRead: Boolean = {
    readPos = 0

    // Only connRead if nothing left over in our buffer from the last time around.
    //      
    val lastRead = if (available <= availablePrev) connRead(buf, available, buf.length - available) else 0
    if (lastRead <= -1) {
      connClose
      return false
    }
     
    availablePrev = available
    available += lastRead
    if (available > buf.length) {
      connClose
      throw new RuntimeException("available larger than buf somehow")
    }

    if (available >= waitingFor) {
      val indexCR: Int = if (available >= 2 &&                 // Optimization to avoid scanning 
                             available == availablePrev + 1) { // the entire buf again for a CR.
                           if (buf(available - 2) == CR) {     // TODO: What if connRead reads a chopped up messages?
                             available - 2                     // TODO: What if connRead gives >1 message?
                           } else
                             -1
                         } else
                           bufIndexOf(available, CR)           // TODO: What if CR is not followed by NL?
      if (indexCR < 0 ||
          indexCR + CRNL.length > available) { // We didn't find a CRNL in our buffer, so
        waitingFor += 1                        // wait for more incoming bytes.
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
          if (bytesNeeded <= 0) {
            if (available > readPos)
              Array.copy(buf, readPos, buf, 0, available - readPos) // Move any unread bytes left over to the front of the buf.
            
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

    false // Returns false if we haven't successfully read and processed a message.
  }       // The caller might invoke us again, though, in a loop.
  
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
     
  def read(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    if (readPos + length > available)
      throw new RuntimeException("reading more bytes than available: " + available)
    Array.copy(buf, readPos, bytes, offset, length)
    readPos += length
  }

  def readDirect(length: Int, recv: (Array[Byte], Int, Int) => Unit): Unit = {
    if (readPos + length > available)
      throw new RuntimeException("reading more bytes than available in readDirect: " + available)
    recv(buf, readPos, length)
    readPos += length
  }
}

