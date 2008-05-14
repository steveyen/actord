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
package ff.actord.client

import java.io._
import java.net._

object Slap {
  val address = InetAddress.getByName("127.0.0.1")
  val port    = 11211
  val CRNL    = "\r\n"

  def main(args: Array[String]) {
    println("simple slap perf test tool: " + args.mkString(" "))

    val n = if (args.length >= 1) args(0).toInt else 10000
    val c = if (args.length >= 2) args(1).toInt else 1

    println("reads per client  : " + n)
    println("number of clients : " + c)

    val startingLine  = new Line
    val finishingLine = new Line

    for (i <- 0 until c)
      (new SlapClient(startingLine, finishingLine, n)).start

    Thread.sleep(500) // Let all the clients get to the starting line.

    val beg = System.currentTimeMillis

    startingLine.inc

    finishingLine.waitUntil(c)

    val end = System.currentTimeMillis

    val duration = (end - beg).asInstanceOf[Double]

    println("total time (sec) : " + duration / 1000.0)
    println("reads / sec      : " + ((1000.0 * n * c) / duration))
  }

  class Line {
    private var f = 0
    def inc = synchronized {
      f += 1
      notifyAll
    }
    def waitUntil(x: Int) = synchronized {
      while (f < x)
        wait
    }
  }

  class SlapClient(startingLine: Line, finishingLine: Line, n: Int) extends Thread {
    override def run = {
      val s = new Socket(address, port)
      val in = s.getInputStream
      val out = s.getOutputStream

      val buf = new Array[Byte](10000)

      val keyPrefix = Math.abs(new java.util.Random().nextInt & 0x0000FFF)
  
      def key(k: String) = keyPrefix + "_" + k
      def skey(k: String) = " " + key(k)  

      val getBytes = ("get" + skey("hello") + CRNL).getBytes
      val nlByte   = '\n'.asInstanceOf[Byte]

      def findByte(x: Byte, offset: Int, length: Int): Int = {
        var j = offset
        while (j < length) {
          if (buf(j) == x)
            return j
          j += 1
        }
        -1
      }

      startingLine.waitUntil(1)

      for (i <- 0 until n) {
        out.write(getBytes)
        out.flush

        val numRead = in.read(buf, 0, buf.length)
        if (numRead <= 0)
          throw new RuntimeException("read error")

        var lineStart = 0

        while (lineStart >= 0 && buf(lineStart) != 'E') { // Huge assumption this is an "END\r\n".
          val nl = findByte(nlByte, lineStart, numRead)
          if (nl < 0)
            throw new RuntimeException("END error")

          lineStart = nl + 1
        }
      }

      finishingLine.inc
    }
  }
}

