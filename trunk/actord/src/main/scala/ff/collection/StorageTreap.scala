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
abstract class StorageTreap[A <% Ordered[A], B <: AnyRef](
  override val root: TreapNode[A, B],
  val io: Storage)
  extends Treap[A, B](root)
{
  def serializeKey(x: A): Array[Byte]
  def unserializeKey(arr: Array[Byte]): A

  def serializeValue(x: B, loc: StorageLoc, appender: StorageLocAppender): Unit
  def unserializeValue(loc: StorageLoc, reader: StorageLocReader): B

  /**
   * The errorValue is called when loading a value, and there's some exception.
   * Possibly because a file was deleted/corrupted/unreadable.  This callback
   * method should return a value representing this error.
   */
  def errorValue(loc: StorageLoc, error: Object): B
  
  /**
   * Subclasses will definitely want to consider overriding this
   * weak priority calculation.  Consider, for example, leveraging
   * the treap's heap-like ability to shuffle high-priority 
   * nodes to the top of the heap for faster access.
   */
  def priority(node: StorageTreapNode[A, B]) = {
    val h = node.key.hashCode
    ((h << 16) & 0xffff0000) | ((h >> 16) & 0x0000ffff)
  }
  
  // --------------------------------------------

  override def mkLeaf(key: A, value: B) = {
    val nodeSwizzle = new StorageSwizzle[TreapNode[A, B]]
    val valSwizzle  = new StorageSwizzle[B]

    valSwizzle.value_!!(value)
    
    val node = mkNode(key, 
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
    case StorageTreapNode(t, k, sv, oldSelf, oldLeft, oldRight) =>
      if (t.io != this.io)
        throw new RuntimeException("treap io mismatch")

      val nodeSwizzle = new StorageSwizzle[TreapNode[A, B]]
      val node        = mkNode(k, sv, 
                               nodeSwizzle,
                               mkNodeSwizzle(left,  oldLeft), 
                               mkNodeSwizzle(right, oldRight))
      nodeSwizzle.value_!!(node)
      node
  }

  def mkNode(key: A,
             swizzleValue: StorageSwizzle[B],
             swizzleSelf:  StorageSwizzle[TreapNode[A, B]],
             swizzleLeft:  StorageSwizzle[TreapNode[A, B]],
             swizzleRight: StorageSwizzle[TreapNode[A, B]]) = // Easy for subclass override.
    StorageTreapNode(this, key, 
                           swizzleValue, 
                           swizzleSelf, 
                           swizzleLeft,
                           swizzleRight)  
  
  def mkNodeSwizzle(next: TreapNode[A, B], 
                    prev: StorageSwizzle[TreapNode[A, B]]): StorageSwizzle[TreapNode[A, B]] = 
    if ((prev != null) &&
        (prev.value eq next))
        prev // Don't create a new swizzle holder, just use old/previous one.
    else next match {
      case e: TreapEmptyNode[A, B] =>
        emptyNodeSwizzle
      case x: StorageTreapNode[A, B] =>
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
    val loc = s.loc // Captured before the following synch block to avoid deadlocks.
    s.valueSync.synchronized {
      val v = s.value
      if (v != null)
          v
      else 
          loadNodeAt(loc, Some(s))
    }
  }
      
  def swizzleSaveNode(s: StorageSwizzle[TreapNode[A, B]]): StorageLoc = {
    val oldLoc = s.loc
    if (oldLoc != null &&
        io.storageLocRefresh(oldLoc) == false)
        oldLoc
    else {
        val newLoc = appendNode(s.value) // Do the saving outside of synchronized for more concurrency.
        s.locSync.synchronized {         // Should be only one writer thread, but check if we were beaten.
          val loc = s.loc
          if (loc != null)
              loc
          else
              s.loc_!!(newLoc)
        }
    }
  }
    
  // --------------------------------------------
  
  def loadNodeAt(loc: StorageLoc,
                 swizzleSelfOpt: Option[StorageSwizzle[TreapNode[A, B]]]): TreapNode[A, B] = {
    if (loc == null)
      throw new RuntimeException("could not load node without a loc")

    if (loc == emptyNodeLoc)
      emptyNode
    else {
      var keyArr: Array[Byte]  = null
      var locValue: StorageLoc = null
      var locLeft: StorageLoc  = null
      var locRight: StorageLoc = null

      try {
        io.readAt(loc, reader => {
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

        val swizzleSelf = swizzleSelfOpt.getOrElse(new StorageSwizzle[TreapNode[A, B]])
        
        val result = StorageTreapNode[A, B](this, 
                                            key, 
                                            swizzleValue,
                                            swizzleSelf,
                                            swizzleLeft,
                                            swizzleRight)

        if (swizzleSelf.loc == null)
            swizzleSelf.loc_!!(loc)
          
        if (swizzleSelf.value == null)
            swizzleSelf.value_!!(result)
                               
        result
      } catch {
        // Perhaps the log files got truncated or corrupted.
        //
        case _ => emptyNode
      }
    }
  }
  
  def appendNode(value: TreapNode[A, B]): StorageLoc =
    value match {
      case e: TreapEmptyNode[A, B] =>
        emptyNodeLoc
      case x: StorageTreapNode[A, B] =>
        val keyArr   = serializeKey(x.key)
        val locValue = swizzleSaveValue(x.swizzleValue)
        val locLeft  = swizzleSaveNode(x.swizzleLeft)
        val locRight = swizzleSaveNode(x.swizzleRight)
        
        io.append((loc, appender) => {
          appender.appendArray(keyArr, 0, keyArr.length)
          appender.appendLoc(locValue)
          appender.appendLoc(locLeft)
          appender.appendLoc(locRight)
        })
    }

  // --------------------------------------------
  
  def swizzleLoadValue(s: StorageSwizzle[B]): B = {
    val loc = s.loc // Captured before the following synch block to avoid deadlocks.
    s.valueSync.synchronized {
      val v = s.value
      if (v != null)
          v
      else
          s.value_!!(loadValueAt(loc))
    }
  }

  def swizzleSaveValue(s: StorageSwizzle[B]): StorageLoc = {
    val oldLoc = s.loc
    if (oldLoc != null &&
        io.storageLocRefresh(oldLoc) == false)
        oldLoc
    else {
        val newLoc = appendValue(s.value) // Do the saving outside of synchronized for more concurrency.
        s.locSync.synchronized {          // Should be only one writer thread, but check if we were beaten.
          val loc = s.loc
          if (loc != null)
              loc
          else
              s.loc_!!(newLoc)
        }
    }
  }

  // --------------------------------------------
  
  def loadValueAt(loc: StorageLoc): B = {
    if (loc == null)
      throw new RuntimeException("could not load value without a loc")

    try {
      io.readAt(loc, reader => unserializeValue(loc, reader))
    } catch {
      case ex => errorValue(loc, ex)
    }
  }

  def appendValue(value: B): StorageLoc = 
    io.append((loc, appender) => serializeValue(value, loc, appender))

  // --------------------------------------------
  
  /**
   * Find the last good treap root in the storage, as marked 
   * by the last permaLoc, and load it.
   *
   * TODO: What about file versioning?
   */
  def loadRootNode(s: StorageWithPermaMarker): Option[TreapNode[A, B]] = {
    try {
      val locSize  = s.storageLocSize
      val locPerma = s.initialPermaMarkerLoc
      if (locPerma.id >= 0 &&
          locPerma.position > locSize) {
        val locRoot = s.readAt(StorageLoc(locPerma.id, locPerma.position - locSize), _.readLoc)
      
        Some(loadNodeAt(locRoot, None))
      } else
        None
    } catch {
      case _ => None
    }
  }  
  
  def appendRootNode(s: StorageWithPermaMarker): StorageLoc = {
    val locRoot = this.appendNode(root)
                
    s.appendWithPermaMarker((loc, appender, permaMarker) => {
      appender.appendLoc(locRoot)
      appender.appendArray(permaMarker, 0, permaMarker.length)
    })
    
    locRoot
  }
}

// ---------------------------------------------------------

/**
 * A treap node that's potentially stored to nonvolatile/persistent 
 * storage.  So, it's evictable from memory.
 */
case class StorageTreapNode[A <% Ordered[A], B <: AnyRef](
  t: StorageTreap[A, B],
  key: A,
  swizzleValue: StorageSwizzle[B],
  swizzleSelf:  StorageSwizzle[TreapNode[A, B]],
  swizzleLeft:  StorageSwizzle[TreapNode[A, B]],
  swizzleRight: StorageSwizzle[TreapNode[A, B]])
  extends TreapFullNode[A, B] 
{
  def left  = t.swizzleLoadNode(swizzleLeft)
  def right = t.swizzleLoadNode(swizzleRight)
  def value = t.swizzleLoadValue(swizzleValue)
  
  override def priority = t.priority(this) // Forward to an easier place for overriding. 
}

