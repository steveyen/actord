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
 * An in-memory, immutable treap implementation in scala.
 *
 * See: http://www.cs.cmu.edu/afs/cs.cmu.edu/project/scandal/public/papers/treaps-spaa98.pdf
 */
class Treap[A <% Ordered[A], B <: AnyRef](val root: TreapNode[A, B])
{
  def this() = this(TreapEmptyNode[A, B])
  
  def mkTreap(r: TreapNode[A, B]): Treap[A, B] = new Treap(r)

  def mkNode(basis: TreapFullNode[A, B], 
             left:  TreapNode[A, B], 
             right: TreapNode[A, B]): TreapNode[A, B] = basis match {
    case TreapMemNode(k, v, _, _) => 
         TreapMemNode(k, v, left, right)
  }

  def union(that: Treap[A, B]): Treap[A, B]     = mkTreap(root.union(this, that.root))
  def intersect(that: Treap[A, B]): Treap[A, B] = mkTreap(root.intersect(this, that.root))
  def diff(that: Treap[A, B]): Treap[A, B]      = mkTreap(root.diff(this, that.root))
  
  override def toString = root.toString
}

// ---------------------------------------------------------

abstract class TreapNode[A <% Ordered[A], B <: AnyRef] 
{
  type T     = Treap[A, B]
  type Node  = TreapNode[A, B]
  type Full  = TreapFullNode[A, B]
  type Empty = TreapEmptyNode[A, B]
  
  def isEmpty: Boolean
  def isLeaf: Boolean
  
  def lookup(t: T, s: A): Node

  /**
   * Splits a treap into two treaps based on a split key "s".
   * The result tuple-3 means (left, X, right), where X is either...
   * null - meaning the key "s" was not in the original treap.
   * non-null - returning the Full node that had key "s".
   * The tuple-3's left treap has keys all < s,
   * and the tuple-3's right treap has keyas all > s.
   */
  def split(t: T, s: A): (Node, Full, Node)

  /**
   * For join to work, we require that "this".keys < "that".keys.
   */
  def join(t: T, that: Node): Node

  /**
   * When union'ed, the values from "that" have precedence 
   * over "this" when there are matching keys.
   */ 
  def union(t: T, that: Node): Node

  /**
   * When intersect'ed, the values from "that" have precedence 
   * over "this" when there are matching keys.
   */ 
  def intersect(t: T, that: Node): Node
  
  /**
   * Works like set-difference, as in "this" minus "that", or this - that.
   */
  def diff(t: T, that: Node): Node
}

// ---------------------------------------------------------

case class TreapEmptyNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B] 
{ 
  def isEmpty: Boolean = true
  def isLeaf: Boolean  = throw new RuntimeException("isLeaf on empty treap node")

  def lookup(t: T, s: A): Node          = this
  def split(t: T, s: A)                 = (this, null, this)
  def join(t: T, that: Node): Node      = that
  def union(t: T, that: Node): Node     = that
  def intersect(t: T, that: Node): Node = this
  def diff(t: T, that: Node): Node      = this
  
  override def toString = "_"
}

// ---------------------------------------------------------

abstract class TreapFullNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B] 
{
  def key: A  
  def left: Node
  def right: Node
  
  def priority = key.hashCode

  def isEmpty: Boolean = false
  def isLeaf: Boolean  = left.isEmpty && right.isEmpty

  def lookup(t: T, s: A): Node = 
    if (s == key)
      this
    else {
      if (s < key)
        left.lookup(t, s)
      else
        right.lookup(t, s)
    }    

  def split(t: T, s: A) = {
    if (s == key) {
      (left, this, right)
    } else {
      if (s < key) {
        if (isLeaf)
          (left, null, this) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = left.split(t, s)
          (l1, m, t.mkNode(this, r1, right))
        }
      } else {
        if (isLeaf)
          (this, null, right) // Optimization when isLeaf.
        else {
          val (l1, m, r1) = right.split(t, s)
          (t.mkNode(this, left, l1), m, r1)
        }
      }
    }
  }
  
  def join(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority)
        t.mkNode(this, left, right.join(t, b))
      else
        t.mkNode(b, this.join(t, b.left), b.right)
  }

  def union(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      if (priority > b.priority) {
        val (l, m, r) = b.split(t, key)
        if (m == null)
          t.mkNode(this, left.union(t, l), right.union(t, r))
        else
          t.mkNode(m, left.union(t, l), right.union(t, r))
      } else {
        val (l, m, r) = this.split(t, b.key)
        
        // Note we don't use m because b (that) has precendence over this when union'ed.
        //
        t.mkNode(b, l.union(t, b.left), r.union(t, b.right))
      }
  }

  def intersect(t: T, that: Node): Node = that match {
    case e: Empty => e
    case b: Full =>
      if (priority > b.priority) {
        val (l, m, r) = b.split(t, key)
        val nl = left.intersect(t, l)
        val nr = right.intersect(t, r)
        if (m == null)
          nl.join(t, nr)
        else
          t.mkNode(m, nl, nr)
      } else {
        val (l, m, r) = this.split(t, b.key)
        val nl = l.intersect(t, b.left)
        val nr = r.intersect(t, b.right)
        if (m == null)
          nl.join(t, nr)
        else
          t.mkNode(b, nl, nr) // The b value has precendence over this value.
      }
  }

  def diff(t: T, that: Node): Node = that match {
    case e: Empty => this
    case b: Full =>
      // TODO: Need to research alternative "diffb" algorithm from scandal...
      //       http://www.cs.cmu.edu/~scandal/treaps/src/treap.sml
      //
      val (l2, m, r2) = b.split(t, key)
      val l = left.diff(t, l2)
      val r = right.diff(t, r2)
      if (m == null)
        t.mkNode(this, l, r)
      else
        l.join(t, r)
  }
}

// ---------------------------------------------------------

case class TreapMemNode[A <% Ordered[A], B <: AnyRef](key: A, value: B, left: TreapNode[A, B], right: TreapNode[A, B]) 
   extends TreapFullNode[A, B] 

// ---------------------------------------------------------

object TreapTest {
  def main(args: Array[String]) {
    val e = TreapEmptyNode[Int, String]
    val t0 = new Treap[Int, String]
    println(t0)
    
    val t1 = new Treap[Int, String](TreapMemNode(1, "100", e, e))
    println(t1)

    val t2 = new Treap[Int, String](TreapMemNode(2, "200", e, e))
    println(t2)
    
    val t1_1 = new Treap[Int, String](TreapMemNode(1, "101", e, e))
    println(t1_1)

    println(t1.union(t2))
    println(t1.union(t2).union(t2))
    println(t1.union(t2).union(t2).union(t1_1))
    println(t1.intersect(t2))
    println(t1.diff(t2))
    println(t2.diff(t1))
    
    val t3 = new Treap[Int, String](TreapMemNode(3, "300", e, e))
    println(t1.union(t2).union(t3))
    println(t1.union(t2).union(t3).intersect(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t1.union(t2)))    
    println(t1.union(t2).union(t3).diff(t2))
  }
}