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
      "should swizzle only deltas" ::
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
    
    def assertHasLoc(n: TreapStorableNode[String, String]) = {
      assertEquals(true,
                   n.swizzleSelf.loc != null)
      assertEquals(true,
                   n.swizzleSelf.loc.position > 0L)
      assertEquals(n,
                   n.swizzleSelf.value)
      assertEquals(true,
                   n.swizzleValue.loc != null)
    }

    try {
      name match {
        case "should be empty after creation" =>
          assertEquals(empty, t.root)
          assertEquals(0L, t.root.count)

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
          assertEquals(1L, t.count)
          assertEquals("top", t.root.first)
          assertEquals("top", t.root.last)
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

          t.swizzleSaveNode(t.rootStorable.swizzleSelf)
          assertHasLoc(t.rootStorable)
          assertHasLoc(t.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])
          assertHasLoc(t.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])

          assertEquals(true,
                       t.rootStorable.swizzleValue.loc.position == 0L)
          assertEquals("333",
                       t.rootStorable.value)
          assertEquals("333",
                       t.rootStorable.swizzleValue.value)

          val swz = new StorageSwizzle[TreapNode[String, String]]
          swz.loc_!!(t.rootStorable.swizzleSelf.loc)
          assertEquals(null, swz.value)

          val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
          assertEquals(3L, t.count)
          assertEquals("1", t.root.first)
          assertEquals("3", t.root.last)
          assertEquals(true, n != null)
          assertEquals(n, swz.value)
          assertEquals(n.key, t.rootStorable.key)
          assertEquals(n.value, t.rootStorable.value)
          assertEquals(3L, n.count)

          assertHasLoc(n)
          assertHasLoc(n.left.
                         asInstanceOf[TreapStorableNode[String, String]])
          assertHasLoc(n.left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])

          assertEquals("2",
                       n.left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         key)
          assertEquals("1",
                       n.left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         key)

          assertEquals("222",
                       n.left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         value)
          assertEquals("111",
                       n.left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         value)

        case "should swizzle only deltas" =>
          t = t.union(t.mkLeaf("1", "111")).
                union(t.mkLeaf("2", "222")).
                union(t.mkLeaf("3", "333")).
                asInstanceOf[TS]

          t.swizzleSaveNode(t.rootStorable.swizzleSelf)
          assertHasLoc(t.rootStorable)
          assertHasLoc(t.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])
          assertHasLoc(t.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])

          assertEquals(true,
                       t.rootStorable.swizzleValue.loc.position == 0L)
                       
          val fLength = f.length
          assertEquals(true,
                       fLength > 0L)

          val t2 = t.union(t.mkLeaf("3", "345")).
                     asInstanceOf[TS]
          
          t.swizzleSaveNode(t2.rootStorable.swizzleSelf)
          assertHasLoc(t2.rootStorable)
          assertHasLoc(t2.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])
          assertHasLoc(t2.rootStorable.
                         left.
                         asInstanceOf[TreapStorableNode[String, String]].
                         left.
                         asInstanceOf[TreapStorableNode[String, String]])

          val f2Length = f.length
          val intSize = 4 // in bytes
          val shortSize = 2
          val longSize = 8
          val locSize = shortSize + longSize
          assertEquals(true,
                       f2Length > fLength)
          assertEquals(true,
                       f2Length < fLength * 2L) // Resaving shouldn't double the size.
          assertEquals(fLength +
                       intSize + "3".length +    // key
                       intSize + "345".length +  // value
                       locSize +                 // valueLoc
                       locSize +                 // leftLoc
                       locSize,                  // rightLoc
                       f2Length)

          val swz = new StorageSwizzle[TreapNode[String, String]]
          swz.loc_!!(t2.rootStorable.swizzleSelf.loc)
          assertEquals(null, swz.value)
          
          val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
          assertEquals(3L, n.count)
          assertEquals("1", t.root.first)
          assertEquals("3", t.root.last)
          assertEquals(empty, n.lookup(t, "0"))
          var x = n.lookup(t, "3").asInstanceOf[TreapStorableNode[String, String]]
          assertEquals("345", x.value)
          x = n.lookup(t, "2").asInstanceOf[TreapStorableNode[String, String]]
          assertEquals("222", x.value)          
          x = n.lookup(t, "1").asInstanceOf[TreapStorableNode[String, String]]
          assertEquals("111", x.value)          
      }
    } finally {
      s.close        
      f.delete
    }
  }
}
