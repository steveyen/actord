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
abstract class TreapStorable[A <% Ordered[A], B <: AnyRef](
  override val root: TreapNode[A, B],
  val io: Storage)
  extends Treap[A, B](root)
{
  override def mkNode(basis: TreapFullNode[A, B], 
                      left:  TreapNode[A, B], 
                      right: TreapNode[A, B]): TreapNode[A, B] = basis match {
    case TreapStorableNode(k, kiMin, kiMax, sn, sv, _, _) =>
      val sn2 = new StorageSwizzle[TreapStorableNode[A, B]]()
      val sv2 = new StorageSwizzle[B]()

      sv2.loc_!!(sv.loc)
      sv2.value_!!(sv.value)

      TreapStorableNode(k, kiMin, kiMax, sn2, sv2, left, right)
  }

  def swizzleNode(s: StorageSwizzle[TreapStorableNode[A, B]]) = {
    s.synchronized {
      if (s.value != null)
          s.value
      else {
        if (s.loc < 0L)
          throw new RuntimeException("could not swizzle load without a loc")
//        s.value_!!(unserialize(io.readArray(s.loc)))
null
      }
    }
  }
  
  def serialize(x: B): Array[Byte]
  def unserialize(arr: Array[Byte]): B

  def swizzleValue(s: StorageSwizzle[B]) = {
    s.synchronized {
      if (s.value != null)
          s.value
      else {
        if (s.loc < 0L)
          throw new RuntimeException("could not swizzle load without a loc")
        s.value_!!(unserialize(io.readArray(s.loc)))
      }
    }
  }
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
  swizzleNode: StorageSwizzle[TreapStorableNode[A, B]],
  swizzleValue: StorageSwizzle[B],
  left: TreapNode[A, B], 
  right: TreapNode[A, B]) 
  extends TreapFullNode[A, B] 
{
  def hasSimpleKey: Boolean = 
    key == keyInnerMin && 
    key == keyInnerMax
  
  def inner(t: T) = 
    t.asInstanceOf[TreapStorable[A, B]].swizzleNode(swizzleNode)

  def value(t: T) = 
    t.asInstanceOf[TreapStorable[A, B]].swizzleValue(swizzleValue)

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
        (l1, m, t.mkNode(this, r1, right))
      }
    } else if (s > keyInnerMax) {
      if (isLeaf)
        (this, null, right) // Optimization when isLeaf.
      else {
        val (l1, m, r1) = right.split(t, s)
        (t.mkNode(this, left, l1), m, r1)
      }
    } else {
      // Split point "s" is without [keyInnerMin, keyInnerMax] range.
      //
      val (l1, m, r1) = inner(t).split(t, s)
      (left.join(t, l1), m, r1.join(t, right))
    }
  }
}

