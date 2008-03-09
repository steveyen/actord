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
    case TreapStorableNode(t, k, sv, oldSelf, oldLeft, oldRight) =>
      if (t.io != this.io)
        throw new RuntimeException("treaps io mismatch")

      val sl = if (oldLeft.value != null &&
                   oldLeft.value == left)
                   oldLeft
               else {
                   val x = new StorageSwizzle[TreapNode[A, B]]()
                   x.value_!!(left.asInstanceOf[TreapNode[A, B]])
                   x
               }
      val sr = if (oldRight.value != null &&
                   oldRight.value == right)
                   oldRight
               else {
                   val x = new StorageSwizzle[TreapNode[A, B]]()
                   x.value_!!(right.asInstanceOf[TreapNode[A, B]])
                   x
               }

      val ss = new StorageSwizzle[TreapStorableNode[A, B]]()

      TreapStorableNode(this, k, sv, ss, sl, sr)
  }

  def swizzleLoadNode(s: StorageSwizzle[TreapNode[A, B]]) = {
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

  def swizzleLoadValue(s: StorageSwizzle[B]) = {
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
  t: TreapStorable[A, B],
  key: A,
  swizzleValue: StorageSwizzle[B],
  swizzleSelf:  StorageSwizzle[TreapStorableNode[A, B]],
  swizzleLeft:  StorageSwizzle[TreapNode[A, B]],
  swizzleRight: StorageSwizzle[TreapNode[A, B]])
  extends TreapFullNode[A, B] 
{
  def left  = t.swizzleLoadNode(swizzleLeft)
  def right = t.swizzleLoadNode(swizzleRight)
  def value = t.swizzleLoadValue(swizzleValue)
}

