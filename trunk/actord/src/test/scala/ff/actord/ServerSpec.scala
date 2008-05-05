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

import scala.collection._

class  MServerSpecTest   extends JUnit3(MServerSpec)
object MServerSpecRunner extends ConsoleRunner(MServerSpec)
object MServerSpec extends Specification with MTestUtil {
  def prep = {
    val m: MServer = new MMainServer
    val ea  = MEntry("a", 0L, 0L, new Array[Byte](0), 0L)
    val ea2 = MEntry("a", 1L, 0L, new Array[Byte](0), 0L)
    (m, ea, ea2)
  }
  
  def assertEquals(msg: String, x: Any, y: Any) =
    x.mustEqual(y)
  def assertEquals(x: Any, y: Any) =
    x.mustEqual(y)
   
  "MServer" should {
    "be empty after creation" in {
      val (m, ea, ea2) = prep

      m.get(List("a")).toList   must beEmpty
      m.delete("a", 0L, false)  must be(false)
      m.get(List("a")).toList   must beEmpty
      m.delete("a", 1L, false)  must be(false)
      m.get(List("a")).toList   must beEmpty
      m.addRep(ea, false, false) must be(false)
      m.get(List("a")).toList   must beEmpty
      m.xpend(ea, true, false)  must be(false)
      m.get(List("a")).toList   must beEmpty
      m.xpend(ea, false, false) must be(false)
      m.get(List("a")).toList   must beEmpty
      m.delta("a", 1L, false)   mustEqual(-1L)
      m.get(List("a")).toList   must beEmpty
      m.checkAndSet(ea, 0L, false) mustEqual("NOT_FOUND")
      m.get(List("a")).toList   must beEmpty
    }
      
    "get after set" in {
      val (m, ea, ea2) = prep

      assertEquals("get 0", true, m.get(List("a")).toList.isEmpty)
      assertEquals("set 1", true, m.set(ea, false))
      assertEquals("get 1", true, entrySame(m.get(List("a")), ea))
      assertEquals("get 1", true, entrySame(m.get(List("a")), ea))
      assertEquals("add x", false, m.addRep(ea, true, false))
    }

    "set values on the same key" in {
      val (m, ea, ea2) = prep

      assertEquals("get 0", true, m.get(List("a")).toList.isEmpty)
      assertEquals("set 1", true, m.set(ea, false))
      assertEquals("get 1", true, entrySame(m.get(List("a")), ea))
      assertEquals("set 2", true, m.set(ea, false))
      assertEquals("get 2", true, entrySame(m.get(List("a")), ea))
      assertEquals("set 3", true, m.set(ea2, false))
      assertEquals("get 3", true, entrySame(m.get(List("a")), ea2))
    }

    "add and replace" in {
      val (m, ea, ea2) = prep

      assertEquals("get 0", true,  m.get(List("a")).toList.isEmpty)
      assertEquals("rep x", false, m.addRep(ea2, false, false))
      assertEquals("get 0", true,  m.get(List("a")).toList.isEmpty)
      assertEquals("add 1", true,  m.addRep(ea, true, false))
      assertEquals("get 1", true,  entrySame(m.get(List("a")), ea))
      assertEquals("add x", false, m.addRep(ea2, true, false))
      assertEquals("get 1", true,  entrySame(m.get(List("a")), ea))
      assertEquals("rep 1", true,  m.addRep(ea, false, false))
      assertEquals("get 1", true,  entrySame(m.get(List("a")), ea))
      assertEquals("rep 2", true,  m.addRep(ea2, false, false))
      assertEquals("get 2", true,  entrySame(m.get(List("a")), ea2))
    }

    "delete with 0 expTime" in {
      val (m, ea, ea2) = prep

      assertEquals("get 0", true,  m.get(List("a")).toList.isEmpty)
      assertEquals("del 0", false, m.delete("a", 0L, false))
      assertEquals("get 0", true,  m.get(List("a")).toList.isEmpty)
      assertEquals("set 1", true,  m.set(ea, false))
      assertEquals("get 1", true,  entrySame(m.get(List("a")), ea))
      assertEquals("del 1", true,  m.delete("a", 0L, false))
      assertEquals("get x", true,  m.get(List("a")).toList.isEmpty)
    }

    "support delta calls" in {
      val (m, ea, ea2) = prep

      assertEquals("get 0", true, m.get(List("a")).toList.isEmpty)
      assertEquals("set 0", true, m.set(simpleEntry("a", "0"), false))
      assertEquals("get 0", true, entrySame(m.get(List("a")), simpleEntry("a", "0")))
      assertEquals("inc 1", 1L,   m.delta("a", 1L, false))
      assertEquals("get x", true, dataSame(m.get(List("a")), simpleEntry("a", "1")))
      assertEquals("inc 1", 2L,   m.delta("a", 1L, false))
      assertEquals("inc 1", 3L,   m.delta("a", 1L, false))
      assertEquals("inc 1", 4L,   m.delta("a", 1L, false))
      assertEquals("get y", true, dataSame(m.get(List("a")), simpleEntry("a", "4")))
      assertEquals("dec 1", 3L,   m.delta("a", -1L, false))
      assertEquals("get z", true, dataSame(m.get(List("a")), simpleEntry("a", "3")))
      assertEquals("dec 1", 2L,   m.delta("a", -1L, false))
      assertEquals("dec 1", 1L,   m.delta("a", -1L, false))
      assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
      assertEquals("get w", true, dataSame(m.get(List("a")), simpleEntry("a", "0")))
      
      // Test underflow.
      //
      assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
      assertEquals("get m", true, dataSame(m.get(List("a")), simpleEntry("a", "0")))
      assertEquals("dec 1", 0L,   m.delta("a", -1L, false))
      assertEquals("get n", true, dataSame(m.get(List("a")), simpleEntry("a", "0")))
    }

    "checkAndSet" in {
      val (m, ea, ea2) = prep

      val c0 = MEntry("c", 0L, 0L, new Array[Byte](0), 0L)
      val c1 = MEntry("c", 1L, 0L, new Array[Byte](0), 1L)
      val c2 = MEntry("c", 2L, 0L, new Array[Byte](0), 2L)

      assertEquals("get 00", true,     m.get(List("c")).toList.isEmpty)
      assertEquals("set c0", true,     m.set(c0, false))
      assertEquals("get c0", true,     entrySame(m.get(List("c")), c0))
      assertEquals("cas ca", "EXISTS", m.checkAndSet(c2, 2L, false))
      assertEquals("get ca", true,     entrySame(m.get(List("c")), c0))
      assertEquals("cas c1", "STORED", m.checkAndSet(c1, 0L, false))
      assertEquals("get c1", true,     entrySame(m.get(List("c")), c1))
    }

    "be empty after flushAll" in {
      val (m, ea, ea2) = prep

      assertEquals(true, m.set(simpleEntry("a1", "0"), false))
      assertEquals(true, m.set(simpleEntry("a2", "0"), false))
      assertEquals(true, m.set(simpleEntry("a3", "0"), false))
      assertEquals(3, m.keys.toList.length)
      m.flushAll(0L)
      Thread.sleep(200) // flushAll is asynchronous.
      assertEquals(0, m.keys.toList.length)
      assertEquals(true, m.get(List("a1")).toList.isEmpty)
      assertEquals(true, m.get(List("a2")).toList.isEmpty)
      assertEquals(true, m.get(List("a3")).toList.isEmpty)
    }

    "getMulti" in {
      val (m, ea, ea2) = prep

      val c0 = MEntry("c0", 0L, 0L, new Array[Byte](0), 0L)
      val c1 = MEntry("c1", 1L, 0L, new Array[Byte](0), 1L)

      assertEquals("get 00", true, m.get(List("c0")).toList.isEmpty)
      assertEquals("get 00", true, m.get(List("c1")).toList.isEmpty)
      assertEquals("get 00", true, m.get(List("c2")).toList.isEmpty)
      
      assertEquals("getMulti 00", true, m.get(List("c0", "c1", "c2")).toList.isEmpty)
      
      assertEquals("set c0", true, m.set(c0, false))
      assertEquals("getMulti 01", true, m.get(List("c0", "c1", "c2")).
                                          toList.map(_.key).sort(_ < _) == List("c0"))

      assertEquals("set c1", true, m.set(c1, false))
      assertEquals("getMulti 02", true, m.get(List("c0", "c1", "c2")).
                                          toList.map(_.key).sort(_ < _) == List("c0", "c1"))

      assertEquals("del c0", true, m.delete("c0", 0L, false))
      assertEquals("getMulti 03", true, m.get(List("c0", "c1", "c2")).
                                          toList.map(_.key).sort(_ < _) == List("c1"))
    }

    "append data correctly" in {
      val (m, ea, ea2) = prep

      val c0 = MEntry("c0", 0L, 0L, "hello".getBytes, 0L)
      val c1 = MEntry("c0", 0L, 0L, "world".getBytes, 0L)
      val c2 = MEntry("c0", 0L, 0L, "there".getBytes, 0L)
      
      assertEquals("get 00", true,  m.get(List("c0")).toList.isEmpty)
      assertEquals("apd 00", false, m.xpend(c0, true, false))
      assertEquals("get 00", true,  m.get(List("c0")).toList.isEmpty)
      assertEquals("set c0", true,  m.set(c0, false))
      assertEquals("get c0", true,  entrySame(m.get(List("c0")), c0))       
      assertEquals("apd c1", true,  m.xpend(c1, true, false))
      assertEquals("get c1", true,  dataEquals(m.get(List("c0")), "helloworld".getBytes))
      assertEquals("apd c2", true,  m.xpend(c2, true, false))
      assertEquals("get c2", true,  dataEquals(m.get(List("c0")), "helloworldthere".getBytes))
    }

    "prepend data correctly" in {
      val (m, ea, ea2) = prep

      val c0 = MEntry("c0", 0L, 0L, "hello".getBytes, 0L)
      val c1 = MEntry("c0", 0L, 0L, "world".getBytes, 0L)
      val c2 = MEntry("c0", 0L, 0L, "there".getBytes, 0L)
      
      assertEquals("get 00", true,  m.get(List("c0")).toList.isEmpty)
      assertEquals("apd 00", false, m.xpend(c0, true, false))
      assertEquals("get 00", true,  m.get(List("c0")).toList.isEmpty)
      assertEquals("set c0", true,  m.set(c0, false))
      assertEquals("get c0", true,  entrySame(m.get(List("c0")), c0))       
      assertEquals("ppd c1", true,  m.xpend(c1, false, false))
      assertEquals("get c1", true,  dataEquals(m.get(List("c0")), "worldhello".getBytes))
      assertEquals("ppd c2", true,  m.xpend(c2, false, false))
      assertEquals("get c2", true,  dataEquals(m.get(List("c0")), "thereworldhello".getBytes))
    }

    "expire entries" in {
      val (m, ea, ea2) = prep

      val c0 = MEntry("c0", 0L, Util.nowInSeconds + 1L, "hello".getBytes, 0L) // Expires in 1 sec.

      assertEquals("get 00", true, m.get(List("c0")).toList.isEmpty)
      assertEquals("set c0", true, m.set(c0, false))
      assertEquals("get c0", true, dataSame(m.get(List("c0")), c0))       
      
      Thread.sleep(2100)
      
      assertEquals("get xx", true, m.get(List("c0")).toList.isEmpty)
    }

    "have some stats" in {
      val (m, ea, ea2) = prep

      val r0: MServerStats = m.stats
      assertEquals("r0 stats", 0L, r0.numEntries)
      assertEquals("r0 stats", 0L, r0.usedMemory)
      assertEquals("r0 stats", 0L, r0.evictions)

      val entrya = MEntry("a", 0L, 0L, new Array[Byte](10), 0L)
      assertEquals("set 1", true, m.set(entrya, false))

      val r1: MServerStats = m.stats
      assertEquals("r1 stats", 1L,  r1.numEntries)
      assertEquals("r1 stats", 10L, r1.usedMemory)
      assertEquals("r1 stats", 0L,  r1.evictions)

      val entryb = MEntry("b", 0L, 0L, new Array[Byte](10), 0L)
      assertEquals("set 2", true, m.set(entryb, false))

      val r2: MServerStats = m.stats
      assertEquals("r2 stats", 2L,  r2.numEntries)
      assertEquals("r2 stats", 20L, r2.usedMemory)
      assertEquals("r2 stats", 0L,  r2.evictions)
    }

    "handle simple benchmark" in {
      val (m, ea, ea2) = prep

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
                       m.get(List(genKey(i))))))
    }

    "simple multithreaded benchmark" in {
      val (m, ea, ea2) = prep

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
    MEntry(key, 0L, 0L, v.getBytes, 0L)
      
  def dataSame(iter: Iterator[MEntry], b: MEntry) =
    iter.toList.
         headOption.
         map(a => (a.data.size == b.data.size) &&
                  (a.data == b.data || a.data.deepEquals(b.data))).
         getOrElse(false)

  def dataEquals(iter: Iterator[MEntry], b: Array[Byte]) =
    iter.toList.
         headOption.
         map(a => (a.data.size == b.size) &&
                  (a.data == b || a.data.deepEquals(b))).
         getOrElse(false)

  def entrySame(iter: Iterator[MEntry], b: MEntry) =
    iter.toList.
         headOption.
         map(a => (a.key == b.key) &&
                  (a.flags == b.flags) &&
                  (a.expTime == b.expTime) &&
                  (a.data.size == b.data.size) &&
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
