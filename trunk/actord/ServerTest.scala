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

import scala.collection._

import scala.testing.SUnit
import scala.testing.SUnit._

/**
 * Tests data methods of MServer, but not any transport/protocol.
 */
class MServerTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty after creation" ::
      "should support get after set" ::
      "should support set calls on the same key" ::
      "should support add and replace operations" ::
      "should support delete calls of 0 expTime" ::
      "should support delta calls" ::
      "should support checkAndSet" ::
      "should be empty after flushAll" ::
      "should support getMulti" ::
      "should append data correctly" ::      
      "should prepend data correctly" ::      
      "should expire entries" ::      
      "simple benchmark" ::
      "simple multithreaded benchmark" ::
      Nil
    ).map(name => new MServerTestCase(name)):_*
  )
}

class MServerTestCase(name: String) extends TestCase(name) with MTestUtil {
  val m: MServer = new MServer

  val ea  = MEntry("a", 0L, 0L, 0, new Array[Byte](0), 0L)
  val ea2 = MEntry("a", 1L, 0L, 0, new Array[Byte](0), 0L)
  
  override def runTest = {
    println("test: " + name)
    name match {
      case "should be empty after creation" =>
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.delete("a", 0L, false))
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.delete("a", 1L, false))      
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.replace(ea, false))
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.xpend(ea, true, false))
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.xpend(ea, false, false))
        assertEquals(None,  m.get("a"))
        assertEquals(-1L,   m.delta("a", 1L, false))
        assertEquals(None,  m.get("a"))
        assertEquals("NOT_FOUND", m.checkAndSet(ea, 0L, false))
        assertEquals(None,  m.get("a"))
        
      case "should support get after set" =>
        assertEquals("get 0", None, m.get("a"))
        assertEquals("set 1", true, m.set(ea, false))
        assertEquals("get 1", true, entrySame(m.get("a"), ea))
        assertEquals("get 1", true, entrySame(m.get("a"), ea))
        assertEquals("add x", false, m.add(ea, false))
  
      case "should support set calls on the same key" =>
        assertEquals("get 0", None, m.get("a"))
        assertEquals("set 1", true, m.set(ea, false))
        assertEquals("get 1", true, entrySame(m.get("a"), ea))
        assertEquals("set 2", true, m.set(ea, false))
        assertEquals("get 2", true, entrySame(m.get("a"), ea))
        assertEquals("set 3", true, m.set(ea2, false))
        assertEquals("get 3", true, entrySame(m.get("a"), ea2))
  
      case "should support add and replace operations" =>
        assertEquals("get 0", None,  m.get("a"))
        assertEquals("rep x", false, m.replace(ea2, false))
        assertEquals("get 0", None,  m.get("a"))
        assertEquals("add 1", true,  m.add(ea, false))
        assertEquals("get 1", true,  entrySame(m.get("a"), ea))
        assertEquals("add x", false, m.add(ea2, false))
        assertEquals("get 1", true,  entrySame(m.get("a"), ea))
        assertEquals("rep 1", true,  m.replace(ea, false))
        assertEquals("get 1", true,  entrySame(m.get("a"), ea))
        assertEquals("rep 2", true,  m.replace(ea2, false))
        assertEquals("get 2", true,  entrySame(m.get("a"), ea2))
  
      case "should support delete calls of 0 expTime" =>
        assertEquals("get 0", None,  m.get("a"))
        assertEquals("del 0", false, m.delete("a", 0L, false))
        assertEquals("get 0", None,  m.get("a"))
        assertEquals("set 1", true,  m.set(ea, false))
        assertEquals("get 1", true,  entrySame(m.get("a"), ea))
        assertEquals("del 1", true,  m.delete("a", 0L, false))
        assertEquals("get x", None,  m.get("a"))
  
      case "should support delta calls" =>
        assertEquals("get 0", None, m.get("a"))
        assertEquals("set 0", true, m.set(simpleEntry("a", "0"), false))
        assertEquals("get 0", true, entrySame(m.get("a"), simpleEntry("a", "0")))
        assertEquals("inc 1", 1L,   m.delta("a", 1L, false))
        assertEquals("get x", true, dataSame(m.get("a"), simpleEntry("a", "1")))
        assertEquals("inc 1", 2L,   m.delta("a", 1L, false))
        assertEquals("inc 1", 3L,   m.delta("a", 1L, false))
        assertEquals("inc 1", 4L,   m.delta("a", 1L, false))
        assertEquals("get y", true, dataSame(m.get("a"), simpleEntry("a", "4")))
        assertEquals("dec 1", 3L,   m.delta("a", -1L, false))
        assertEquals("get z", true, dataSame(m.get("a"), simpleEntry("a", "3")))
        assertEquals("dec 1", 2L,   m.delta("a", -1L, false))
        assertEquals("dec 1", 1L,   m.delta("a", -1L, false))
        assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
        assertEquals("get w", true, dataSame(m.get("a"), simpleEntry("a", "0")))
        
        // Test underflow.
        //
        assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
        assertEquals("get m", true, dataSame(m.get("a"), simpleEntry("a", "0")))
        assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
        assertEquals("get n", true, dataSame(m.get("a"), simpleEntry("a", "0")))
        
      case "should support checkAndSet" =>
        val c0 = MEntry("c", 0L, 0L, 0, new Array[Byte](0), 0L)
        val c1 = MEntry("c", 1L, 0L, 0, new Array[Byte](0), 1L)
        val c2 = MEntry("c", 2L, 0L, 0, new Array[Byte](0), 2L)
  
        assertEquals("get 00", None,     m.get("c"))
        assertEquals("set c0", true,     m.set(c0, false))
        assertEquals("get c0", true,     entrySame(m.get("c"), c0))
        assertEquals("cas ca", "EXISTS", m.checkAndSet(c2, 2L, false))
        assertEquals("get ca", true,     entrySame(m.get("c"), c0))
        assertEquals("cas c1", "STORED", m.checkAndSet(c1, 0L, false))
        assertEquals("get c1", true,     entrySame(m.get("c"), c1))
        
      case "should be empty after flushAll" =>
        assertEquals(true, m.set(simpleEntry("a1", "0"), false))
        assertEquals(true, m.set(simpleEntry("a2", "0"), false))
        assertEquals(true, m.set(simpleEntry("a3", "0"), false))
        assertEquals(3, m.keys.toList.length)
        m.flushAll(0L)
        Thread.sleep(2) // flushAll is asynchronous.
        assertEquals(0, m.keys.toList.length)
        assertEquals(None, m.get("a1"))
        assertEquals(None, m.get("a2"))
        assertEquals(None, m.get("a3"))
      
      case "should support getMulti" =>
        val c0 = MEntry("c0", 0L, 0L, 0, new Array[Byte](0), 0L)
        val c1 = MEntry("c1", 1L, 0L, 0, new Array[Byte](0), 1L)
  
        assertEquals("get 00", None, m.get("c0"))
        assertEquals("get 00", None, m.get("c1"))
        assertEquals("get 00", None, m.get("c2"))
        
        assertEquals("getMulti 00", true, m.getMulti(List("c0", "c1", "c2")).toList.isEmpty)
        
        assertEquals("set c0", true, m.set(c0, false))
        assertEquals("getMulti 01", true, m.getMulti(List("c0", "c1", "c2")).
                                            toList.map(_.key).sort(_ < _) == List("c0"))

        assertEquals("set c1", true, m.set(c1, false))
        assertEquals("getMulti 02", true, m.getMulti(List("c0", "c1", "c2")).
                                            toList.map(_.key).sort(_ < _) == List("c0", "c1"))

        assertEquals("del c0", true, m.delete("c0", 0L, false))
        assertEquals("getMulti 03", true, m.getMulti(List("c0", "c1", "c2")).
                                            toList.map(_.key).sort(_ < _) == List("c1"))

      case "should append data correctly" =>
        val c0 = MEntry("c0", 0L, 0L, 5, "hello".getBytes, 0L)
        val c1 = MEntry("c0", 0L, 0L, 5, "world".getBytes, 0L)
        val c2 = MEntry("c0", 0L, 0L, 5, "there".getBytes, 0L)
        
        assertEquals("get 00", None,  m.get("c0"))
        assertEquals("apd 00", false, m.xpend(c0, true, false))
        assertEquals("get 00", None,  m.get("c0"))
        assertEquals("set c0", true,  m.set(c0, false))
        assertEquals("get c0", true,  entrySame(m.get("c0"), c0))       
        assertEquals("apd c1", true,  m.xpend(c1, true, false))
        assertEquals("get c1", true,  dataEquals(m.get("c0"), "helloworld".getBytes))
        assertEquals("apd c2", true,  m.xpend(c2, true, false))
        assertEquals("get c2", true,  dataEquals(m.get("c0"), "helloworldthere".getBytes))

      case "should prepend data correctly" =>
        val c0 = MEntry("c0", 0L, 0L, 5, "hello".getBytes, 0L)
        val c1 = MEntry("c0", 0L, 0L, 5, "world".getBytes, 0L)
        val c2 = MEntry("c0", 0L, 0L, 5, "there".getBytes, 0L)
        
        assertEquals("get 00", None,  m.get("c0"))
        assertEquals("apd 00", false, m.xpend(c0, true, false))
        assertEquals("get 00", None,  m.get("c0"))
        assertEquals("set c0", true,  m.set(c0, false))
        assertEquals("get c0", true,  entrySame(m.get("c0"), c0))       
        assertEquals("ppd c1", true,  m.xpend(c1, false, false))
        assertEquals("get c1", true,  dataEquals(m.get("c0"), "worldhello".getBytes))
        assertEquals("ppd c2", true,  m.xpend(c2, false, false))
        assertEquals("get c2", true,  dataEquals(m.get("c0"), "thereworldhello".getBytes))

      case "should expire entries" =>
        val c0 = MEntry("c0", 0L, Util.nowInSeconds + 1L, 5, "hello".getBytes, 0L) // Expires in 1 sec.

        assertEquals("get 00", None, m.get("c0"))
        assertEquals("set c0", true, m.set(c0, false))
        assertEquals("get c0", true, dataSame(m.get("c0"), c0))       
        
        Thread.sleep(2100)
        
        assertEquals("get xx", None, m.get("c0"))
        
      case "simple benchmark" =>
        val n = 4000
        println(calc(n, "set",
                     benchMarkAvgMillis(1, 
                       for (i <- 0 until n)
                         m.set(simpleEntry(genKey(i), i.toString), false))))
        println(calc(n, "re-set",
                     benchMarkAvgMillis(10, 
                       for (i <- 0 until n)
                         m.set(simpleEntry(genKey(i), i.toString), false))))
        println(calc(n, "get",
                     benchMarkAvgMillis(10, 
                       for (i <- 0 until n)
                         m.get(genKey(i)))))
  
      case "simple multithreaded benchmark" => 
        val nOperations = 4000
        val nThreads    = 5
        val results     = new Array[String](nThreads)
  
        (0 until nThreads).map(
          x => {
            val t = new Thread {
                      var resultId: Int = -1
                      override def run = {
                        results(resultId) = calc(nOperations, "set",
                                              benchMarkAvgMillis(1, 
                                                for (i <- 0 until nOperations)
                                                  m.set(simpleEntry(genKey(i), i.toString), false)))
                      }
                    }
            t.resultId = x
            t.start
            t
          }
        ).foreach(_.join)
        
        results.foreach(println _)
    }
  }
}

// -------------------------------------------

trait MTestUtil {
  def p(o: Option[MEntry]) =
    println(asString(o))
    
  def asString(o: Option[MEntry]) =
    o + "->" + o.map(e => new String(e.data, "US-ASCII")).getOrElse("")
  
  def simpleEntry(key: String, v: String) =
    MEntry(key, 0L, 0L, v.getBytes.size, v.getBytes, 0L)
      
  def dataSame(aOpt: Option[MEntry], b: MEntry) =
    aOpt.map(a => (a.dataSize == b.dataSize) &&
                  (a.data == b.data || a.data.deepEquals(b.data))).
         getOrElse(false)

  def dataEquals(aOpt: Option[MEntry], b: Array[Byte]) =
    aOpt.map(a => (a.dataSize == b.size) &&
                  (a.data == b || a.data.deepEquals(b))).
         getOrElse(false)

  def entrySame(aOpt: Option[MEntry], b: MEntry) =
    aOpt.map(a => (a.key == b.key) &&
                  (a.flags == b.flags) &&
                  (a.expTime == b.expTime) &&
                  (a.dataSize == b.dataSize) &&
                  (a.data == b.data || a.data.deepEquals(b.data)) &&
                  (a.cid == b.cid)).
         getOrElse(false)
         
  def genKey(i: Int): String = { // Simple deterministic algorithm.
    var r = new StringBuffer      
    var a = 1
    for (x <- i until (i + 4)) {
      a = a * x
      r = r.append(a)
    }
    r.reverse.toString
  }
  
  def calc(n: Int, verb: String, result: Long) =
    "msecs to " + verb + " " + n + " keys: " + result + 
    "\n  " + verb + "s/sec: " + ((1000 * n) / result)

  def benchMark(repeat: Int, f: => Unit): List[Long] =
    (0 until repeat).map(
      i => {
        val startTime = System.currentTimeMillis
        f
        val endTime = System.currentTimeMillis
        
        endTime - startTime      
      }
    ).toList
  
  def benchMarkAvgMillis(repeat: Int, f: => Unit) = {
    val results = benchMark(repeat, f)
    results.foldLeft(0L)(_ + _) / results.length.toLong
  }

  def benchMarkAvgSecs(repeat: Int, f: => Unit) = 
      benchMarkAvgMillis(repeat, f) / 1000
}
