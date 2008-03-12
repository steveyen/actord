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
  extends TreapNodeFactory[A, B]
     with immutable.SortedMap[A, B]
{
  def this() = this(TreapEmptyNode[A, B])
  
  def mkTreap(r: TreapNode[A, B]): Treap[A, B] = new Treap(r)
  
  def mkLeaf(k: A, v: B): TreapNode[A, B] =
    TreapMemNode(k, v, 
                 TreapEmptyNode[A, B], 
                 TreapEmptyNode[A, B])  

  def mkNode(basis: TreapFullNode[A, B], 
             left:  TreapNode[A, B], 
             right: TreapNode[A, B]): TreapNode[A, B] = basis match {
    case TreapMemNode(k, v, _, _) => 
         TreapMemNode(k, v, left, right)
  }

  def union(that: Treap[A, B]): Treap[A, B]     = union(that.root)
  def intersect(that: Treap[A, B]): Treap[A, B] = intersect(that.root)
  def diff(that: Treap[A, B]): Treap[A, B]      = diff(that.root)
  
  def union(that: TreapNode[A, B]): Treap[A, B]     = mkTreap(root.union(this, that))
  def intersect(that: TreapNode[A, B]): Treap[A, B] = mkTreap(root.intersect(this, that))
  def diff(that: TreapNode[A, B]): Treap[A, B]      = mkTreap(root.diff(this, that))
  
  lazy val count = root.count // TODO: Revisit treap size/count.

  def size: Int = count.toInt

  override def empty[C]: immutable.SortedMap[A, C] = 
    throw new RuntimeException("TODO: empty method is unimplemented")
  
  override def get(key: A): Option[B] = 
    root.lookup(this, key) match {
      case n: TreapFullNode[A, B] => Some(n.value)
      case _ => None
    }
    
  def elements: Iterator[Pair[A, B]] = root.elements(this).elements

  override def rangeImpl(from: Option[A], until: Option[A]): immutable.SortedMap[A, B] = 
    mkTreap(root.range(this, from, until))
    
  override def update[B1 >: B](key: A, value: B1): immutable.SortedMap[A, B1] =
    value match {
      case v: B => upd(key, v)
      case _ => throw new RuntimeException("TODO: update on type B1 unimplemented")
    }
  
  def upd(key: A, value: B): Treap[A, B] =  
    union(mkLeaf(key, value))

  override def - (key: A): immutable.SortedMap[A, B] = 
    del(key)

  def del(key: A): Treap[A, B] = 
    mkTreap(root.del(this, key))
  
  override def firstKey: A = root.firstKey
  override def lastKey: A  = root.lastKey
  
  override def compare(k0: A, k1: A): Int = k0.compare(k1)
    
  override def toString = root.toString
}

// ---------------------------------------------------------

abstract class TreapNodeFactory[A <% Ordered[A], B <: AnyRef]  {
  def mkLeaf(k: A, v: B): TreapNode[A, B]

  def mkNode(basis: TreapFullNode[A, B], 
             left:  TreapNode[A, B], 
             right: TreapNode[A, B]): TreapNode[A, B]
}

// ---------------------------------------------------------

abstract class TreapNode[A <% Ordered[A], B <: AnyRef] 
{
  type T     = TreapNodeFactory[A, B]
  type Node  = TreapNode[A, B]
  type Full  = TreapFullNode[A, B]
  type Empty = TreapEmptyNode[A, B]
  
  def isEmpty: Boolean
  def isLeaf: Boolean
  
  def count: Long
  def firstKey: A
  def lastKey: A
  
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

  def elements(t: T): immutable.ImmutableIterator[Pair[A, B]]
  
  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B]

  def del(t: T, k: A): TreapNode[A, B]
}

// ---------------------------------------------------------

case class TreapEmptyNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B] 
{ 
  def isEmpty: Boolean = true
  def isLeaf: Boolean  = throw new RuntimeException("isLeaf on empty treap node")

  def count = 0L
  def firstKey = throw new NoSuchElementException("empty treap")
  def lastKey  = throw new NoSuchElementException("empty treap")
  
  def lookup(t: T, s: A): Node          = this
  def split(t: T, s: A)                 = (this, null, this)
  def join(t: T, that: Node): Node      = that
  def union(t: T, that: Node): Node     = that
  def intersect(t: T, that: Node): Node = this
  def diff(t: T, that: Node): Node      = this
  
  def elements(t: T): immutable.ImmutableIterator[Pair[A, Nothing]] = 
                      immutable.ImmutableIterator.empty

  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B] = this
  
  def del(t: T, k: A): TreapNode[A, B] = this

  override def toString = "_"
}

// ---------------------------------------------------------

abstract class TreapFullNode[A <% Ordered[A], B <: AnyRef] extends TreapNode[A, B] 
{
  def key: A  
  def left: Node
  def right: Node
  def value: B
  
  def priority = key.hashCode

  def isEmpty: Boolean = false
  def isLeaf: Boolean  = left.isEmpty && right.isEmpty
  
  def count = 1L + left.count + right.count
  def firstKey = if (left.isEmpty)  key else left.firstKey
  def lastKey  = if (right.isEmpty) key else right.lastKey

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

  def elements(t: T): immutable.ImmutableIterator[Pair[A, B]] = 
    left.elements(t).append(Pair(key, value), () => right.elements(t))

  def range(t: T, from: Option[A], until: Option[A]): TreapNode[A, B] = {
    if (from  == None && until == None)    return this
    if (from  != None && key < from.get)   return right.range(t, from, until)
    if (until != None && key >= until.get) return left.range(t, from, until)
    
    val (l1, m1, r1) = from.map(s => left.split(t, s)).
                            getOrElse(null, null, left)
    val (l2, m2, r2) = until.map(s => right.split(t, s)).
                             getOrElse(right, null, null)
    if (m1 == null)
      t.mkNode(this, r1, l2)
    else
      t.mkNode(m1, TreapEmptyNode[A, B],
                   TreapEmptyNode[A, B]).
        join(t, t.mkNode(this, r1, l2))
  }

  def del(t: T, k: A): TreapNode[A, B] = {
    val (l, m, r) = split(t, k)
    l.join(t, r)
  }
}

// ---------------------------------------------------------

case class TreapMemNode[A <% Ordered[A], B <: AnyRef](key: A, value: B, left: TreapNode[A, B], right: TreapNode[A, B]) 
   extends TreapFullNode[A, B] 

