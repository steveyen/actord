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
  def main(args: Array[String]) {
    def address = InetAddress.getByName("127.0.0.1")
    def port    = 11211
  
    val CRNL = "\r\n"
  
    val s = new Socket(address, port)
    val in = new BufferedReader(new InputStreamReader(s.getInputStream))
    val out = new PrintWriter(s.getOutputStream, true)

    def line(x: String) = x + CRNL

    def r = in.readLine
  
    def w(x: String) = {
      out.write(line(x))
      out.flush
    }

    val keyPrefix = Math.abs(new java.util.Random().nextInt & 0x0000FFF)
  
    def key(k: String) = keyPrefix + "_" + k
    def skey(k: String) = " " + key(k)
  
    println("actord slap perf test tool: " + args.mkString(" "))

    val n = if (args.length > 0) args(0).toInt else 10000

    println("iterations: " + n)

    val beg = System.currentTimeMillis

    for (i <- 0 until n) {
      w("get" + skey("hello"))
      r // "END"
    }

    val end = System.currentTimeMillis

    val duration = (end - beg).asInstanceOf[Double]

    println("total time (sec): " + duration / 1000.0)
    println("reads / sec: " + ((1000.0 * n) / duration))
  }
}

