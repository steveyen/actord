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
 * Tests persistent treap storage.
 */
class TreapStorableTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty after creation" ::
      "should swizzle one node" ::
      "should swizzle a few nodes" ::
      Nil
    ).map(name => new TreapStorableTestCase(name)) :_*
  )
}

class TreapStorableTestCase(name: String) extends TestCase(name) {
  val empty = TreapEmptyNode[String, String]
  
  class TS(override val root: TreapNode[String, String],
           override val io: Storage)
    extends TreapStorable[String, String](root, io) {
    override def mkTreap(r: TreapNode[String, String]): Treap[String, String] = 
      new TS(r, io)    
    
    def serializeKey(x: String): Array[Byte]     = x.getBytes
    def unserializeKey(arr: Array[Byte]): String = new String(arr)
  
    def serializeValue(x: String): Array[Byte]     = x.getBytes
    def unserializeValue(arr: Array[Byte]): String = new String(arr)
    
    def rootStorable = root.asInstanceOf[TreapStorableNode[String, String]]
  }
  
  override def runTest = {
    println("test: " + name)

    val f = File.createTempFile("test_treapstorable", ".tmp")
    val s = new SingleFileStorage(f)
    var t = new TS(empty, s)
    
    def assertInMemOnly(n: TreapStorableNode[String, String], k: String, v: String) = {
      assertEquals(k, n.key)
      assertEquals(v, n.value)
      assertEquals(null, n.swizzleValue.loc)
      assertEquals(v,    n.swizzleValue.value)
      assertEquals(null, n.swizzleSelf.loc)
      assertEquals(n,    n.swizzleSelf.value)
    }

    try {
      name match {
        case "should be empty after creation" =>
          assertEquals(empty, t.root)

        case "should swizzle one node" =>
          t = t.union(t.mkLeaf("top", "root")).asInstanceOf[TS]
          assertInMemOnly(t.rootStorable, "top", "root")

          t.swizzleSaveNode(t.rootStorable.swizzleSelf)
          assertEquals(true,
                       t.rootStorable.swizzleSelf.loc != null)
          assertEquals(true,
                       t.rootStorable.swizzleSelf.loc.position > 0L)
          assertEquals(t.rootStorable,
                       t.rootStorable.swizzleSelf.value)
          assertEquals(true,
                       t.rootStorable.swizzleValue.loc != null)
          assertEquals(true,
                       t.rootStorable.swizzleValue.loc.position == 0L)
          assertEquals("root",
                       t.rootStorable.swizzleValue.value)

          val swz = new StorageSwizzle[TreapNode[String, String]]
          swz.loc_!!(t.rootStorable.swizzleSelf.loc)
          assertEquals(null, swz.value)

          val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
          assertEquals(true, n != null)
          assertEquals(n, swz.value)
          assertEquals(n.key, t.rootStorable.key)
          assertEquals(n.value, t.rootStorable.value)

        case "should swizzle a few nodes" =>
          t = t.union(t.mkLeaf("1", "111")).
                union(t.mkLeaf("2", "222")).
                union(t.mkLeaf("3", "333")).
                asInstanceOf[TS]

          assertInMemOnly(t.rootStorable, "3", "333")
          assertInMemOnly(t.rootStorable.
                            left.
                            asInstanceOf[TreapStorableNode[String, String]], 
                          "2", "222")
          assertInMemOnly(t.rootStorable.
                            left.
                            asInstanceOf[TreapStorableNode[String, String]].
                            left.
                            asInstanceOf[TreapStorableNode[String, String]], 
                          "1", "111")
/*
          t.swizzleSaveNode(t.rootStorable.swizzleSelf)
          assertEquals(true,
                       t.rootStorable.swizzleSelf.loc != null)
          assertEquals(true,
                       t.rootStorable.swizzleSelf.loc.position > 0L)
          assertEquals(t.rootStorable,
                       t.rootStorable.swizzleSelf.value)
          assertEquals(true,
                       t.rootStorable.swizzleValue.loc != null)
          assertEquals(true,
                       t.rootStorable.swizzleValue.loc.position == 0L)
          assertEquals("root",
                       t.rootStorable.swizzleValue.value)

          val swz = new StorageSwizzle[TreapNode[String, String]]
          swz.loc_!!(t.rootStorable.swizzleSelf.loc)
          assertEquals(null, swz.value)

          val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
          assertEquals(true, n != null)
          assertEquals(n, swz.value)
          assertEquals(n.key, t.rootStorable.key)
          assertEquals(n.value, t.rootStorable.value)
*/
/*
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
          assertEquals(TreapMemNode(3, "300", 
                         TreapMemNode(2, "200", 
                           TreapMemNode(1, "100", e, e), 
                           e), 
                         e),
                       t.root)
  
          t = t1.union(t2).union(t3).intersect(t1.union(t2))
          assertEquals(TreapMemNode(2, "200", 
                         TreapMemNode(1, "100", e, e), 
                         e), 
                       t.root)
          
          t = t1.union(t2).union(t3).diff(t1.union(t2))
          assertEquals(TreapMemNode(3, "300", e, e), 
                       t.root)
          
          t = t1.union(t2).union(t3).diff(t2)
          assertEquals(TreapMemNode(3, "300", 
                         TreapMemNode(1, "100", e, e), 
                         e), 
                       t.root)
*/
      }
    } finally {
      s.close        
      f.delete
    }
  }
}
