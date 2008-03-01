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
      "simple benchmark" ::
      "simple multithreaded benchmark" ::
      Nil
    ).map(name => new MServerTestCase(name)):_*
  )
}

/**
 * Tests data methods of MServer, but not any transport/protocol.
 */
class MServerTestCase(name: String) extends TestCase(name) with MTestUtil {
  val m: MServer = new MServer(new immutable.TreeMap[String, MEntry])

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
        assertEquals(false, m.append(ea, false))
        assertEquals(None,  m.get("a"))
        assertEquals(false, m.prepend(ea, false))
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
