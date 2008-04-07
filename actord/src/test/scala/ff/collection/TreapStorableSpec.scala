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

import org.specs._
import org.specs.runner._

import scala.collection._

import java.io._

class  TreapStorableSpecTest   extends JUnit3(TreapStorableSpec)
object TreapStorableSpecRunner extends ConsoleRunner(TreapStorableSpec)
object TreapStorableSpec extends Specification {
  val empty = TreapEmptyNode[String, String]

  class TS(override val root: TreapNode[String, String],
           override val io: Storage)
    extends TreapStorable[String, String](root, io) {
    override def mkTreap(r: TreapNode[String, String]): Treap[String, String] = 
      new TS(r, io)    
    
    def serializeKey(x: String): Array[Byte]     = x.getBytes
    def unserializeKey(arr: Array[Byte]): String = new String(arr)
  
    def serializeValue(x: String, loc: StorageLoc, appender: StorageLocAppender): Unit = {
      val arr = x.getBytes
      appender.appendArray(arr, 0, arr.length)
    }
      
    def unserializeValue(loc: StorageLoc, reader: StorageLocReader): String = 
      new String(reader.readArray)
    
    def rootStorable = root.asInstanceOf[TreapStorableNode[String, String]]
  }
  
  def assertInMemOnly(n: TreapStorableNode[String, String], k: String, v: String) = {
    k must_== n.key
    v must_== n.value
    n.swizzleValue.loc   must be(null)
    n.swizzleValue.value must_== v
    n.swizzleSelf.loc    must be(null)
    n.swizzleSelf.value  must_== n
  }
  
  def assertHasLoc(n: TreapStorableNode[String, String]) = {
    n.swizzleSelf.loc          must notBe(null)
    n.swizzleSelf.loc.position must beStrictlyGreaterThan(0L)
    n.swizzleSelf.value        must be_==(n)
    n.swizzleValue.loc         must notBe(null)
  }
  
  def prep = {
    val f = File.createTempFile("test_treapstorable", ".tmp")
    val s = new FileStorage(f)
    val t = new TS(empty, s)
    (f, s, t)
  }

  "TreapStorable" should {
    "be empty after creation" in {
      var (f, s, t) = prep
      try {
        t.root       mustEqual(empty)
        t.root.count mustEqual(0L)
      } finally {
        s.close        
        f.delete
      }
    }

    "swizzle one node" in {
      var (f, s, t) = prep
      try {
        t = t.union(t.mkLeaf("top", "root")).asInstanceOf[TS]
        assertInMemOnly(t.rootStorable, "top", "root")

        t.swizzleSaveNode(t.rootStorable.swizzleSelf)
        t.rootStorable.swizzleSelf.loc          must notBe(null)
        t.rootStorable.swizzleSelf.loc.position must beStrictlyGreaterThan(0L)
        t.rootStorable must beEqual(t.rootStorable.swizzleSelf.value)
        t.rootStorable.swizzleValue.loc          must notBe(null)
        t.rootStorable.swizzleValue.loc.position mustEqual(0L)
        t.rootStorable.swizzleValue.value        mustEqual("root")

        val swz = new StorageSwizzle[TreapNode[String, String]]
        swz.loc_!!(t.rootStorable.swizzleSelf.loc)
        swz.value mustEqual(null)

        val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
        t.count mustEqual(1L)
        "top" mustEqual(t.root.firstKey)
        "top" mustEqual(t.root.lastKey)
        n must notBe(null)
        n mustEqual(swz.value)
        n.key   mustEqual(t.rootStorable.key)
        n.value mustEqual(t.rootStorable.value)
      } finally {
        s.close        
        f.delete
      }
    }

    "swizzle a few nodes" in {
      var (f, s, t) = prep
      try {
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

        t.rootStorable.swizzleValue.loc.position mustEqual(0L)
        "333" mustEqual(t.rootStorable.value)
        "333" mustEqual(t.rootStorable.swizzleValue.value)

        val swz = new StorageSwizzle[TreapNode[String, String]]
        swz.loc_!!(t.rootStorable.swizzleSelf.loc)
        swz.value mustEqual(null)

        val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
        3L  mustEqual(t.count)
        "1" mustEqual(t.root.firstKey)
        "3" mustEqual(t.root.lastKey)
        n must notBe(null)
        n mustEqual(swz.value)
        n.key   mustEqual(t.rootStorable.key)
        n.value mustEqual(t.rootStorable.value)
        3L mustEqual(n.count)

        assertHasLoc(n)
        assertHasLoc(n.left.
                       asInstanceOf[TreapStorableNode[String, String]])
        assertHasLoc(n.left.
                       asInstanceOf[TreapStorableNode[String, String]].
                       left.
                       asInstanceOf[TreapStorableNode[String, String]])

        "2" mustEqual(n.left.
                        asInstanceOf[TreapStorableNode[String, String]].
                        key)
        "1" mustEqual(n.left.
                        asInstanceOf[TreapStorableNode[String, String]].
                        left.
                        asInstanceOf[TreapStorableNode[String, String]].
                        key)

        "222" mustEqual(n.left.
                          asInstanceOf[TreapStorableNode[String, String]].
                          value)
        "111" mustEqual(n.left.
                          asInstanceOf[TreapStorableNode[String, String]].
                          left.
                          asInstanceOf[TreapStorableNode[String, String]].
                          value)
      } finally {
        s.close        
        f.delete
      }
    }

    "swizzle only deltas" in {
      var (f, s, t) = prep
      try {
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

        t.rootStorable.swizzleValue.loc.position mustEqual(0L)
                     
        val fLength = f.length
        fLength must beStrictlyGreaterThan(0L)

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
        val locSize = intSize + longSize
        f2Length must beStrictlyGreaterThan(fLength)
        f2Length must beStrictlyLessThan(fLength * 2L) // Resaving shouldn't double the size.
        f2Length mustEqual(fLength +
                           intSize + "3".length +    // key
                           intSize + "345".length +  // value
                           locSize +                 // valueLoc
                           locSize +                 // leftLoc
                           locSize)                  // rightLoc

        val swz = new StorageSwizzle[TreapNode[String, String]]
        swz.loc_!!(t2.rootStorable.swizzleSelf.loc)
        swz.value mustEqual(null)
        
        val n = t.swizzleLoadNode(swz).asInstanceOf[TreapStorableNode[String, String]]
        3L  mustEqual(n.count)
        "1" mustEqual(t.root.firstKey)
        "3" mustEqual(t.root.lastKey)
        empty mustEqual(n.lookup(t, "0"))
        var x = n.lookup(t, "3").asInstanceOf[TreapStorableNode[String, String]]
        "345" mustEqual(x.value)
        x = n.lookup(t, "2").asInstanceOf[TreapStorableNode[String, String]]
        "222" mustEqual(x.value)          
        x = n.lookup(t, "1").asInstanceOf[TreapStorableNode[String, String]]
        "111" mustEqual(x.value)          
      } finally {
        s.close        
        f.delete
      }
    }
  }
}