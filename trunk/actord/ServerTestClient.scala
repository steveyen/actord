package ff.actord

import java.io._
import java.net._

import ff.actord.Util._

import scala.testing.SUnit
import scala.testing.SUnit._

/**
 * Tests the transport/protocol of an MServer with a simple, blocking client.
 */
class MServerTestClient extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty for new keys" ::
      "should have version" ::
      "should support set and get for a key" ::
      "should support delete" ::
      Nil
    ).map(name => new MServerTestClientCase(name)):_*
  )
}

class MServerTestClientCase(name: String) extends TestCase(name) {
  def address = InetAddress.getByName("localhost")
  def port    = 11211
  
  override def runTest = {
    println("test: " + name)

    val s = new Socket(address, port)
    val in = new BufferedReader(new InputStreamReader(s.getInputStream))
    val out = new PrintWriter(s.getOutputStream, true)
  
    def r(expect: String) = {
      val line = in.readLine
      println("client received " + line)
      assertEquals(expect, line)
    }
  
    def w(x: String) = {
      out.write(line(x))
      out.flush
    }
      
    try {
      name match {
        case "should be empty for new keys" =>
          w("get" + skey("a"))
          r("END")
          w("get" + skey("a") + skey("b") + skey("c"))
          r("END")
          
        case "should have version" =>
          w("version")
          val x = in.readLine
          assertEquals(true, x.startsWith("VERSION "))
          assertEquals(true, x.length > ("VERSION ".length + 1))
         
        case "should support set and get for a key" =>
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
  
        case "should support delete" =>
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
      }
    } finally {
      if (!s.isClosed)
        s.close
    }
  }

  val keyPrefix = Math.abs(new java.util.Random().nextInt & 0x0000FFF)
  
  def key(k: String) = keyPrefix + "_" + k
  def skey(k: String) = " " + key(k)
  
  def line(x: String) = x + CRNL
}

