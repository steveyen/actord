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
package ff.collection

import scala.collection._

import scala.testing.SUnit
import scala.testing.SUnit._

import java.io._

/**
 * Tests treap methods.
 */
class TreapTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should handle simple treap operations" ::
      "should handle range operations" ::
      Nil
    ).map(name => new TreapTestCase(name)) :_*
  )
}

class TreapTestCase(name: String) extends TestCase(name) {
  override def runTest = {
    println("test: " + name)
    name match {
      case "should handle simple treap operations" =>
        val e = TreapEmptyNode[Int, String]
        val t0 = new Treap[Int, String]
        assertEquals(e, t0.root)
        
        val t1 = new Treap[Int, String](TreapMemNode(1, "100", e, e))
        assertEquals(TreapMemNode(1, "100", e, e), t1.root)
    
        val t2 = new Treap[Int, String](TreapMemNode(2, "200", e, e))
        assertEquals(TreapMemNode(2, "200", e, e), t2.root)
        
        val t1_1 = new Treap[Int, String](TreapMemNode(1, "101", e, e))
        assertEquals(TreapMemNode(1, "101", e, e), t1_1.root)
        
        var t = t1.union(t2)
        assertEquals(TreapMemNode(2, "200", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)
        
        t = t1.union(t2).union(t2)
        assertEquals(TreapMemNode(2, "200", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)
        
        t = t1.union(t2).union(t2).union(t1_1)
        assertEquals(1, t.root.firstKey)
        assertEquals(2, t.root.lastKey)
        assertEquals(TreapMemNode(2, "200", 
                       TreapMemNode(1, "101", e, e), 
                       e), 
                     t.root)
        
        t = t1.intersect(t2)
        assertEquals(e, 
                     t.root)
        
        t = t1.diff(t2)
        assertEquals(TreapMemNode(1, "100", e, e), 
                     t.root)

        t = t2.diff(t1)
        assertEquals(TreapMemNode(2, "200", e, e), 
                     t.root)
        
        val t3 = new Treap[Int, String](TreapMemNode(3, "300", e, e))

        t = t1.union(t2).union(t3)
        assertEquals(3L, t.root.count)
        assertEquals(1, t.root.firstKey)
        assertEquals(3, t.root.lastKey)
        assertEquals(TreapMemNode(3, "300", 
                       TreapMemNode(2, "200", 
                         TreapMemNode(1, "100", e, e), 
                         e), 
                       e),
                     t.root)
        
        t = t1.union(t2).union(t3).intersect(t1.union(t2))
        assertEquals(2L, t.root.count)
        assertEquals(1, t.root.firstKey)
        assertEquals(2, t.root.lastKey)
        assertEquals(TreapMemNode(2, "200", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)
        
        t = t1.union(t2).union(t3).diff(t1.union(t2))
        assertEquals(1L, t.root.count)
        assertEquals(3, t.root.firstKey)
        assertEquals(3, t.root.lastKey)
        assertEquals(TreapMemNode(3, "300", e, e), 
                     t.root)
        
        t = t1.union(t2).union(t3).diff(t2)
        assertEquals(2L, t.root.count)
        assertEquals(1, t.root.firstKey)
        assertEquals(3, t.root.lastKey)
        assertEquals(TreapMemNode(3, "300", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)

        t = t1.union(t2).union(t3).-(2).asInstanceOf[t1.type]
        assertEquals(2L, t.root.count)
        assertEquals(1, t.root.firstKey)
        assertEquals(3, t.root.lastKey)
        assertEquals(TreapMemNode(3, "300", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)

        t = t1.update(2, "200").update(3, "300").-(2).asInstanceOf[t1.type]
        assertEquals(2L, t.root.count)
        assertEquals(1, t.root.firstKey)
        assertEquals(3, t.root.lastKey)
        assertEquals(TreapMemNode(3, "300", 
                       TreapMemNode(1, "100", e, e), 
                       e), 
                     t.root)

      case "should handle range operations" =>
    }
  }
}
