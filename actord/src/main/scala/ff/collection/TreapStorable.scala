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
 * Concrete subclasses need to implement serialize/unserialize
 * of keys and values.
 */
abstract class TreapStorable[A <% Ordered[A], B <: AnyRef](
  override val root: TreapNode[A, B],
  val io: Storage)
  extends Treap[A, B](root)
{
  def serializeKey(x: A): Array[Byte]
  def unserializeKey(arr: Array[Byte]): A

  def serializeValue(x: B, loc: StorageLoc, appender: StorageLocAppender): Unit
  def unserializeValue(loc: StorageLoc, reader: StorageLocReader): B
  
  // --------------------------------------------

  override def mkLeaf(key: A, value: B) = {
    val nodeSwizzle = new StorageSwizzle[TreapNode[A, B]]
    val valSwizzle  = new StorageSwizzle[B]

    valSwizzle.value_!!(value)
    
    val node = TreapStorableNode(this, 
                                 key, 
                                 valSwizzle, 
                                 nodeSwizzle, 
                                 emptyNodeSwizzle,
                                 emptyNodeSwizzle)
                                 
    nodeSwizzle.value_!!(node)
    
    node
  }   

  override def mkNode(basis: TreapFullNode[A, B], 
                      left:  TreapNode[A, B], 
                      right: TreapNode[A, B]): TreapNode[A, B] = basis match {
    case TreapStorableNode(t, k, sv, oldSelf, oldLeft, oldRight) =>
      if (t.io != this.io)
        throw new RuntimeException("treap io mismatch")

      val nodeSwizzle = new StorageSwizzle[TreapNode[A, B]]
      val node        = TreapStorableNode(this, k, sv, 
                          nodeSwizzle,
                          mkNodeSwizzle(left,  oldLeft), 
                          mkNodeSwizzle(right, oldRight))
      nodeSwizzle.value_!!(node)
      node
  }
  
  def mkNodeSwizzle(next: TreapNode[A, B], 
                    prev: StorageSwizzle[TreapNode[A, B]]): StorageSwizzle[TreapNode[A, B]] = 
    if (prev != null &&
        prev.value == next)
        prev // Don't create a new swizzle holder, just use old/previous one.
    else next match {
      case e: TreapEmptyNode[A, B] =>
        emptyNodeSwizzle
      case x: TreapStorableNode[A, B] =>
        x.swizzleSelf
    }

  // --------------------------------------------
  
  val emptyNodeSwizzle = {
    val x = new StorageSwizzle[TreapNode[A, B]]
    x.loc_!!(emptyNodeLoc)
    x.value_!!(emptyNode)
    x
  }
  
  val emptyNode    = TreapEmptyNode[A, B]
  def emptyNodeLoc = NullStorageLoc

  // --------------------------------------------
  
  def swizzleLoadNode(s: StorageSwizzle[TreapNode[A, B]]): TreapNode[A, B] = {
    s.synchronized {
      if (s.value != null)
          s.value
      else {
        if (s.loc == null)
          throw new RuntimeException("could not swizzle load without a loc")
        if (s.loc == emptyNodeLoc)
          s.value_!!(emptyNode)
        else {
          var keyArr: Array[Byte]  = null
          var locValue: StorageLoc = null
          var locLeft: StorageLoc  = null
          var locRight: StorageLoc = null

          io.readAt(s.loc, reader => {
            keyArr   = reader.readArray
            locValue = reader.readLoc
            locLeft  = reader.readLoc
            locRight = reader.readLoc
          })

          val key          = unserializeKey(keyArr)
          val swizzleValue = new StorageSwizzle[B]
          val swizzleLeft  = new StorageSwizzle[TreapNode[A, B]]
          val swizzleRight = new StorageSwizzle[TreapNode[A, B]]

          swizzleValue.loc_!!(locValue)
          swizzleLeft.loc_!!(locLeft)
          swizzleRight.loc_!!(locRight)
          
          s.value_!!(TreapStorableNode[A, B](this, 
                                             key, 
                                             swizzleValue,
                                             s,
                                             swizzleLeft,
                                             swizzleRight))
        }
      }
    }
  }
  
  def swizzleSaveNode(s: StorageSwizzle[TreapNode[A, B]]): StorageLoc = s.synchronized {
    if (s.loc != null)
        s.loc
    else {
      s.value match {
        case e: TreapEmptyNode[A, B] =>
          s.loc_!!(emptyNodeLoc)
        case x: TreapStorableNode[A, B] =>
          val keyArr   = serializeKey(x.key)
          val locValue = swizzleSaveValue(x.swizzleValue)
          val locLeft  = swizzleSaveNode(x.swizzleLeft)
          val locRight = swizzleSaveNode(x.swizzleRight)
          
          s.loc_!!(io.append((loc, appender) => {
            appender.appendArray(keyArr, 0, keyArr.length)
            appender.appendLoc(locValue)
            appender.appendLoc(locLeft)
            appender.appendLoc(locRight)
          }))
      }
    }
  }

  // --------------------------------------------
  
  def swizzleLoadValue(s: StorageSwizzle[B]): B = 
    s.synchronized {
      if (s.value != null)
          s.value
      else {
        if (s.loc == null)
          throw new RuntimeException("could not swizzle load without a loc")
          
        s.value_!!(io.readAt(s.loc, reader => unserializeValue(s.loc, reader)))
      }
    }

  def swizzleSaveValue(s: StorageSwizzle[B]): StorageLoc = 
    s.synchronized {
      if (s.loc != null)
          s.loc
      else 
          s.loc_!!(io.append((loc, appender) => serializeValue(s.value, loc, appender)))
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
  swizzleSelf:  StorageSwizzle[TreapNode[A, B]],
  swizzleLeft:  StorageSwizzle[TreapNode[A, B]],
  swizzleRight: StorageSwizzle[TreapNode[A, B]])
  extends TreapFullNode[A, B] 
{
  lazy val left  = t.swizzleLoadNode(swizzleLeft)
  lazy val right = t.swizzleLoadNode(swizzleRight)
  lazy val value = t.swizzleLoadValue(swizzleValue)
}

