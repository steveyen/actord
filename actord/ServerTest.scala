package ff.actord

import scala.testing.SUnit
import scala.testing.SUnit._

class MServerTest extends TestConsoleMain {
  def suite = new TestSuite(
    new MServerTestCase("does SUnit work?"),    
    new MServerTestCase("does SUnit work?")
  )
}

class MServerTestCase(n: String) extends TestCase(n) {
 override def runTest = n match {
   case "does SUnit work?" => 
    println("hi!")
    assertTrue(true)
    assertTrue(true)
 }
}
