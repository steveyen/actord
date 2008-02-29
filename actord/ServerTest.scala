package ff.actord

import scala.collection._

import scala.testing.SUnit
import scala.testing.SUnit._

class MServerTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty when created" ::
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
    case "should be empty when created" =>
      assertEquals(m.get("a"), None)
      assertEquals(m.delete("a", 0L), false)
      assertEquals(m.delete("a", 1L), false)      
      assertEquals(m.replace(ea), false)
      assertEquals(m.delta("a", 1L), "NOT_FOUND")
      assertEquals(m.append(ea), false)
      assertEquals(m.prepend(ea), false)
      assertEquals(m.checkAndSet(ea, 0L), "NOT_FOUND")
      
    case "does SUnit work?" => 
      assertTrue(true)
  }
}
