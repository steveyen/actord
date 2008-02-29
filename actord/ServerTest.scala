package ff.actord

import scala.collection._

import scala.testing.SUnit
import scala.testing.SUnit._

class MServerTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty after creation" ::
      "should support get after set" ::
      Nil
    ).map(name => new MServerTestCase(name)):_*
  )
}

class MServerTestCase(name: String) extends TestCase(name) {
  val m: MServer = new MServer(new immutable.TreeMap[String, MEntry])

  val ea = MEntry("a", 0L, 0L, 0, new Array[Byte](0), 0L)
  val eb = MEntry("b", 0L, 0L, 0, new Array[Byte](0), 0L)
  val ec = MEntry("c", 0L, 0L, 0, new Array[Byte](0), 0L)
      
  override def runTest = name match {
    case "should be empty after creation" =>
      assertEquals(None,  m.get("a"))
      assertEquals(false, m.delete("a", 0L))
      assertEquals(false, m.delete("a", 1L))      
      assertEquals(false, m.replace(ea))
      assertEquals(false, m.append(ea))
      assertEquals(false, m.prepend(ea))
      assertEquals("NOT_FOUND", m.delta("a", 1L))
      assertEquals("NOT_FOUND", m.checkAndSet(ea, 0L))
      
    case "should support get after set" =>
      assertEquals("first get", None, m.get("a"))
      assertEquals("first set", true, m.set(ea))
      assertTrue("get after set 1", entrySame(m.get("a"), ea))
      assertTrue("get after set 2", entrySame(m.get("a"), ea))
      assertEquals("no add after set", false, m.add(ea))
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
