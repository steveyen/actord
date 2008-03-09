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
 * Extends the immutable treap implementation with persistence.
 */
class TreapStorable[A <% Ordered[A], B <: AnyRef](override val root: TreapNode[A, B])
  extends Treap[A, B](root)
{
  def this() = this(TreapEmptyNode[A, B])
  
  override def mkTreap(r: TreapNode[A, B]): Treap[A, B] = new TreapStorable(r)
}

// ---------------------------------------------------------

/**
 * A treap node that's potentially stored to nonvolatile/persistent 
 * storage.  So, it's evictable from memory.
 */
case class TreapStorableNode[A <% Ordered[A], B <: AnyRef](
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
  
  def value(t: T): B                       = swizzleLoad(t).value
  def inner(t: T): TreapStorableNode[A, B] = swizzleLoad(t).node

  def swizzleLoad(t: T) = synchronized {
    // Swizzle/load from storage, if not already.
    //
    swizzle.synchronized {
      if (swizzle.node == null) {
      }
    }
    swizzle
  }

  override def lookup(t: T, s: A): Node = 
    if (s < keyInnerMin)
      left.lookup(t, s)
    else if (s > keyInnerMax)
      right.lookup(t, s)
    else 
      inner(t).lookup(t, s)

  override def split(t: T, s: A) = {
    if (s < keyInnerMin) {
      if (isLeaf)
        (left, null, this) // Optimization when isLeaf.
      else {
        val (l1, m, r1) = left.split(t, s)
        (l1, m, mkNode(this, r1, right))
      }
    } else if (s > keyInnerMax) {
      if (isLeaf)
        (this, null, right) // Optimization when isLeaf.
      else {
        val (l1, m, r1) = right.split(t, s)
        (mkNode(this, left, l1), m, r1)
      }
    } else {
      // Split point "s" is without [keyInnerMin, keyInnerMax] range.
      //
      val (l1, m, r1) = inner(t).split(t, s)
      (left.join(t, l1), m, r1.join(t, right))
    }
  }
}

class TreapStorageNodeSwizzle[A <% Ordered[A], B <: AnyRef] {
  type Storable = TreapStorableNode[A, B]

  private var loc_i: Long      = -1L 
  private var node_i: Storable = null
  private var value_i: B       = _
  
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

  def value: B = synchronized { value_i }
  
  def value_!!(x: B) = synchronized { 
    value_i = x; 
    this 
  }
}

