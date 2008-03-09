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

/**
 * Treap implementation in scala.
 *
 * See: http://www.cs.cmu.edu/afs/cs.cmu.edu/project/scandal/public/papers/treaps-spaa98.pdf
 */
class Treap[A <% Ordered[A], B](val root: TreapNode[A, B])
{
  def this() = this(TreapEmptyNode[A, B])
  
  def mkTreap(r: TreapNode[A, B]): Treap[A, B] = new Treap(r)
  
  def union(that: Treap[A, B]): Treap[A, B]     = mkTreap(root.union(that.root))
  def intersect(that: Treap[A, B]): Treap[A, B] = mkTreap(root.intersect(that.root))
  def diff(that: Treap[A, B]): Treap[A, B]      = mkTreap(root.diff(that.root))
  
  override def toString = root.toString
}

abstract class TreapNode[A <% Ordered[A], B] 
{
  type Node  = TreapNode[A, B]
  type Full  = TreapFullNode[A, B]
  type Empty = TreapEmptyNode[A, B]
  
  def isEmpty: Boolean
  def isLeaf: Boolean

  def split(s: A): (Node, Full, Node)

  /**
   * For join to work, we require that "this".keys < "that".keys.
   */
  def join(that: Node): Node

  /**
   * When union'ed, the values from "that" have precedence 
   * over "this" when there are matching keys.
   */ 
  def union(that: Node): Node

  /**
   * When intersect'ed, the values from "that" have precedence 
   * over "this" when there are matching keys.
   */ 
  def intersect(that: Node): Node
  
  /**
   * Works like set-difference, as in "this" minus "that", or this - that.
   */
  def diff(that: Node): Node
}

case class TreapFullNode[A <% Ordered[A], B](key: A, value: B, left: TreapNode[A, B], right: TreapNode[A, B]) 
   extends TreapNode[A, B] 
{
  def mkFull(key: A, value: B, left: Node, right: Node): Node = 
        TreapFullNode[A, B](key, value, left, right)
  
  def priority = key.hashCode

  def isEmpty: Boolean = false
  def isLeaf: Boolean  = left.isEmpty && right.isEmpty

  def split(s: A) = {
    if (s == key) {
      (left, this, right)
    } else {
      if (s < key) {
        if (isLeaf)
          (left, null, this) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = left.split(s)
          (l1, m, mkFull(key, value, r1, right))
        }
      } else {
        if (isLeaf)
          (this, null, right) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = right.split(s)
          (mkFull(key, value, left, l1), m, r1)
        }
      }
    }
  }
  
  def join(that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority)
        mkFull(key, value, left, right.join(b))
      else
        mkFull(b.key, b.value, this.join(b.left), b.right)
  }

  def union(that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority) {
        val (l, m, r) = b.split(key)
        if (m == null)
          mkFull(key, value, left.union(l), right.union(r))
        else
          mkFull(m.key, m.value, left.union(l), right.union(r))
      } else {
        val (l, m, r) = this.split(b.key)
        
        // Note we don't use m because b (that) has precendence over this when union'ed.
        //
        mkFull(b.key, b.value, l.union(b.left), r.union(b.right))
      }
  }

  def intersect(that: Node): Node = that match {
    case e: Empty => e
    case b: Full =>
      if (priority > b.priority) {
        val (l, m, r) = b.split(key)
        val nl = left.intersect(l)
        val nr = right.intersect(r)
        if (m == null)
          nl.join(nr)
        else
          mkFull(m.key, m.value, nl, nr)
      } else {
        val (l, m, r) = this.split(b.key)
        val nl = l.intersect(b.left)
        val nr = r.intersect(b.right)
        if (m == null)
          nl.join(nr)
        else
          mkFull(b.key, b.value, nl, nr) // The b value has precendence over this value.
      }
  }

  def diff(that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      // TODO: Need to research alternative "diffb" algorithm from scandal...
      //       http://www.cs.cmu.edu/~scandal/treaps/src/treap.sml
      //
      val (l2, m, r2) = b.split(key)
      val l = left.diff(l2)
      val r = right.diff(r2)
      if (m == null)
        mkFull(key, value, l, r)
      else
        l.join(r)
  }
}

case class TreapEmptyNode[A <% Ordered[A], B] extends TreapNode[A, B] 
{ 
  def isEmpty: Boolean = true
  def isLeaf: Boolean  = throw new RuntimeException("isLeaf on empty treap node")

  def split(s: A) = (this, null, this)
  def join(that: Node): Node      = that
  def union(that: Node): Node     = that
  def intersect(that: Node): Node = this
  def diff(that: Node): Node      = this
  
  override def toString = "_"
}

// ---------------------------------------------------------

object TreapTest {
  def main(args: Array[String]) {
    val e = TreapEmptyNode[Int, Int]
    val t0 = new Treap[Int, Int]
    println(t0)
    
    val t1 = new Treap[Int, Int](TreapFullNode(1, 100, e, e))
    println(t1)

    val t2 = new Treap[Int, Int](TreapFullNode(2, 200, e, e))
    println(t2)
    
    val t1_1 = new Treap[Int, Int](TreapFullNode(1, 101, e, e))
    println(t1_1)

    println(t1.union(t2))
    println(t1.union(t2).union(t2))
    println(t1.union(t2).union(t2).union(t1_1))
    println(t1.intersect(t2))
    println(t1.diff(t2))
    println(t2.diff(t1))
    
    val t3 = new Treap[Int, Int](TreapFullNode(3, 300, e, e))
    println(t1.union(t2).union(t3))
    println(t1.union(t2).union(t3).intersect(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t2))
  }
}
