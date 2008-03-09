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
 * Immutable treap implementation in scala.
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

// ---------------------------------------------------------

abstract class TreapNode[A <% Ordered[A], B] 
{
  type Node  = TreapNode[A, B]
  type Full  = TreapFullNode[A, B]
  type Empty = TreapEmptyNode[A, B]
  
  def isEmpty: Boolean
  def isLeaf: Boolean
  
  def lookup(s: A): Node

  /**
   * Splits a treap into two treaps based on a split key "s".
   * The result tuple-3 means (left, X, right), where X is either...
   * null - meaning the key "s" was not in the original treap.
   * non-null - returning the Full node that had key "s".
   * The tuple-3's left treap has keys all < s,
   * and the tuple-3's right treap has keyas all > s.
   */
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

// ---------------------------------------------------------

case class TreapEmptyNode[A <% Ordered[A], B] extends TreapNode[A, B] 
{ 
  def isEmpty: Boolean = true
  def isLeaf: Boolean  = throw new RuntimeException("isLeaf on empty treap node")

  def lookup(s: A): Node          = this
  def split(s: A)                 = (this, null, this)
  def join(that: Node): Node      = that
  def union(that: Node): Node     = that
  def intersect(that: Node): Node = this
  def diff(that: Node): Node      = this
  
  override def toString = "_"
}

// ---------------------------------------------------------

abstract class TreapFullNode[A <% Ordered[A], B] extends TreapNode[A, B] 
{
  def key: A  
  def left: Node
  def right: Node
  
  def mkNode(basis: Full, left: Node, right: Node): Node
  
  def priority = key.hashCode

  def isEmpty: Boolean = false
  def isLeaf: Boolean  = left.isEmpty && right.isEmpty

  def lookup(s: A): Node = 
    if (s == key)
      this
    else {
      if (s < key)
        left.lookup(s)
      else
        right.lookup(s)
    }    

  def split(s: A) = {
    if (s == key) {
      (left, this, right)
    } else {
      if (s < key) {
        if (isLeaf)
          (left, null, this) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = left.split(s)
          (l1, m, mkNode(this, r1, right))
        }
      } else {
        if (isLeaf)
          (this, null, right) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = right.split(s)
          (mkNode(this, left, l1), m, r1)
        }
      }
    }
  }
  
  def join(that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority)
        mkNode(this, left, right.join(b))
      else
        mkNode(b, this.join(b.left), b.right)
  }

  def union(that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority) {
        val (l, m, r) = b.split(key)
        if (m == null)
          mkNode(this, left.union(l), right.union(r))
        else
          mkNode(m, left.union(l), right.union(r))
      } else {
        val (l, m, r) = this.split(b.key)
        
        // Note we don't use m because b (that) has precendence over this when union'ed.
        //
        mkNode(b, l.union(b.left), r.union(b.right))
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
          mkNode(m, nl, nr)
      } else {
        val (l, m, r) = this.split(b.key)
        val nl = l.intersect(b.left)
        val nr = r.intersect(b.right)
        if (m == null)
          nl.join(nr)
        else
          mkNode(b, nl, nr) // The b value has precendence over this value.
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
        mkNode(this, l, r)
      else
        l.join(r)
  }
}

// ---------------------------------------------------------

/**
 * An in-memory treap node implementation.
 */
case class TreapMemNode[A <% Ordered[A], B](key: A, value: B, left: TreapNode[A, B], right: TreapNode[A, B]) 
   extends TreapFullNode[A, B] 
{
  def mkNode(basis: Full, left: Node, right: Node): Node = basis match {
    case TreapMemNode(k, v, _, _) => 
         TreapMemNode(k, v, left, right)
  }
}

// ---------------------------------------------------------

/**
 * A treap node that's potentially stored to nonvolatile/persistent 
 * storage.  So, it's evictable from memory.
 */
case class TreapStorableNode[A <% Ordered[A], B](
  key: A,
  keyInnerMin: A,
  keyInnerMax: A,
  swizzle: TreapStorageNodeSwizzle[A, B],
  left: TreapNode[A, B], 
  right: TreapNode[A, B]) 
  extends TreapFullNode[A, B] 
{
  def mkNode(basis: Full, left: Node, right: Node): Node = basis match {
    case TreapStorableNode(k, kiMin, kiMax, s, _, _) =>
         TreapStorableNode(k, kiMin, kiMax, s, left, right)
  }
  
  def hasSimpleKey: Boolean = 
    key == keyInnerMin && 
    key == keyInnerMax
  
  def value: B = inner.value
  
  def inner: TreapStorableNode[A, B] = synchronized {
    // Swizzle/load from storage, if not already.
    //
    swizzle.synchronized {
      if (swizzle.node == null) {
      }
      swizzle.node
    }
  }

  override def lookup(s: A): Node = 
    if (s < keyInnerMin)
      left.lookup(s)
    else if (s > keyInnerMax)
      right.lookup(s)
    else 
      inner.lookup(s)

  override def split(s: A) = {
    if (s < keyInnerMin) {
      if (isLeaf)
        (left, null, this) // Optimization when isLeaf.
      else {
        val (l1, m, r1) = left.split(s)
        (l1, m, mkNode(this, r1, right))
      }
    } else if (s > keyInnerMax) {
      if (isLeaf)
        (this, null, right) // Optimization when isLeaf.
      else {
        val (l1, m, r1) = right.split(s)
        (mkNode(this, left, l1), m, r1)
      }
    } else {
      // Split point "s" is without [keyInnerMin, keyInnerMax] range.
      //
      val (l1, m, r1) = inner.split(s)
      (left.join(l1), m, r1.join(right))
    }
  }
}

class TreapStorageNodeSwizzle[A <% Ordered[A], B] {
  type Storable = TreapStorableNode[A, B]

  private var loc_i: Long      = -1L 
  private var node_i: Storable = null
  
  def loc: Long = synchronized { loc_i }
  
  def loc_!!(x: Long) = synchronized { 
    if (loc_i >= 0L && x >= 0L)
      throw new RuntimeException("cannot override an existing swizzle loc")

    loc_i = x; 
    this 
  }

  /**
   * The storable node here must have a simple key (hasSimpleKey == true).
   */
  def node: Storable = synchronized { node_i }
  
  def node_!!(x: Storable) = synchronized { 
    if (x != null) {
      if (x.hasSimpleKey == false)
        throw new RuntimeException("swizzle node must have simple key")
      if (node_i != null)
        throw new RuntimeException("cannot overwrite an existing swizzle node")
    }        
      
    node_i = x; 
    this 
  }
}

// ---------------------------------------------------------

object TreapTest {
  def main(args: Array[String]) {
    val e = TreapEmptyNode[Int, Int]
    val t0 = new Treap[Int, Int]
    println(t0)
    
    val t1 = new Treap[Int, Int](TreapMemNode(1, 100, e, e))
    println(t1)

    val t2 = new Treap[Int, Int](TreapMemNode(2, 200, e, e))
    println(t2)
    
    val t1_1 = new Treap[Int, Int](TreapMemNode(1, 101, e, e))
    println(t1_1)

    println(t1.union(t2))
    println(t1.union(t2).union(t2))
    println(t1.union(t2).union(t2).union(t1_1))
    println(t1.intersect(t2))
    println(t1.diff(t2))
    println(t2.diff(t1))
    
    val t3 = new Treap[Int, Int](TreapMemNode(3, 300, e, e))
    println(t1.union(t2).union(t3))
    println(t1.union(t2).union(t3).intersect(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t2))
  }
}
