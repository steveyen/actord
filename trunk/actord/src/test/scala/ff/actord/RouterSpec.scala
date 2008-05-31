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

import org.specs._
import org.specs.runner._

import java.io._
import java.net._

import ff.actord.Util._

/**
 * Tests the transport/protocol of an MServerRouter with a simple, blocking client.
 */
class  RouterWireSpecTest   extends JUnit3(RouterWireSpec)
object RouterWireSpecRunner extends ConsoleRunner(RouterWireSpec)
object RouterWireSpec extends Specification with MTestUtil {
  def address = InetAddress.getByName("127.0.0.1")
  def port    = 11222

  // Fire up a router that connects to a target server started already in a previous test (ServerWireSpec).
  //
  ff.actord.Router.main(new Array[String](0)) 
  
  def prep = {
    val s = new Socket(address, port)
    val in = new BufferedReader(new InputStreamReader(s.getInputStream))
    val out = new PrintWriter(s.getOutputStream, true)

    def r(expect: String) = {
      val line = in.readLine
      println("rclient received " + line)
      assertEquals(expect, line)
    }
  
    def w(x: String) = {
      val y = line(x)
      out.write(y)
      out.flush
    }
      
    (s, in, out, r _, w _)
  }
  
  def assertEquals(msg: String, x: Any, y: Any) =
    x.mustEqual(y)
  def assertEquals(x: Any, y: Any) =
    x.mustEqual(y)
   
  "MServerRouter wire protocol" should {
    "be empty for new keys" in {
      val (s, in, out, r, w) = prep
      try {
        w("get" + skey("a"))
        r("END")
        w("get" + skey("a") + skey("b") + skey("c"))
        r("END")
      } finally {
        if (!s.isClosed)
          s.close
      }
    }
        
    "have version" in {
      val (s, in, out, r, w) = prep
      try {
        w("version")
        val x = in.readLine
        assertEquals(true, x.startsWith("VERSION "))
        assertEquals(true, x.length > ("VERSION ".length + 1))
      } finally {
        if (!s.isClosed)
          s.close
      }
    }
        
    "support set and get for a key" in {
      val (s, in, out, r, w) = prep
      try {
        w("get" + skey("a"))
        r("END")
        w("set" + skey("a") + " 0 0 5")
        w("hello")
        r("STORED")
        w("get" + skey("a"))
        r("VALUE" + skey("a") + " 0 5")
        r("hello")
        r("END")
        w("get" + skey("a") + skey("b"))
        r("VALUE" + skey("a") + " 0 5")
        r("hello")
        r("END")
      } finally {
        if (!s.isClosed)
          s.close
      }
    }
        
    "support delete" in {
      val (s, in, out, r, w) = prep
      try {
        w("set" + skey("a") + " 0 0 5")
        w("hello")
        r("STORED")
        w("get" + skey("a"))
        r("VALUE" + skey("a") + " 0 5")
        r("hello")
        r("END")
        w("delete" + skey("a") + " 0")
        r("DELETED")
        w("get" + skey("a"))
        r("END")
        w("delete" + skey("a") + " 0")
        r("NOT_FOUND")
        w("get" + skey("a"))
        r("END")
      } finally {
        if (!s.isClosed)
          s.close
      }
    }
  }

  val keyPrefix = Math.abs(new java.util.Random().nextInt & 0x0000FFF)
  
  def key(k: String) = keyPrefix + "_" + k
  def skey(k: String) = " " + key(k)
  
  def line(x: String) = x + CRNL
}

