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
      Nil
    ).map(name => new MServerTestCase(name)):_*
  )
}

class MServerTestCase(name: String) extends TestCase(name) {
  val m: MServer = new MServer(new immutable.TreeMap[String, MEntry])

  val ea  = MEntry("a", 0L, 0L, 0, new Array[Byte](0), 0L)
  val ea2 = MEntry("a", 1L, 0L, 0, new Array[Byte](0), 0L)
      
  override def runTest = name match {
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
      assertEquals("NOT_FOUND", m.delta("a", 1L, false))
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
  }
  
  def entrySame(aOpt: Option[MEntry], b: MEntry) =
    aOpt.map(a => (a.key == b.key) &&
                  (a.flags == b.flags) &&
                  (a.expTime == b.expTime) &&
                  (a.dataSize == b.dataSize) &&
                  (a.data == b.data || a.data.deepEquals(b.data)) &&
                  (a.cid == b.cid)).
         getOrElse(false)
}
