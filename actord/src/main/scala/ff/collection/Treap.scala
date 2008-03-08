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
package ff.actord

import scala.collection._

/**
 * Treap implementation in scala.
 *
 * See: http://www.cs.cmu.edu/afs/cs.cmu.edu/project/scandal/public/papers/treaps-spaa98.pdf
 */
class Treap[A <% Ordered[A], B <% AnyRef](val root: Treap[A, B]#Node)
{
  abstract class Node {
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
  
  def mkFull(key: A, value: B, left: Node, right: Node): Node = 
        Full(key, value, left, right)

  case class Full(key: A, value: B, left: Node, right: Node) extends Node {
    def priority = key.hashCode

    def split(s: A) = {
      if (s == key) {
        (left, this, right)
      } else {
        if (s < key) {
          val (l1, m, r1) = left.split(s)
          (l1, m, mkFull(key, value, r1, right))
        } else {
          val (l1, m, r1) = right.split(s)
          (mkFull(key, value, left, l1), m, r1)
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
  
  case class Empty extends Node { 
    def split(s: A)                 = (this, null, this)
    def join(that: Node): Node      = that
    def union(that: Node): Node     = that
    def intersect(that: Node): Node = this
    def diff(that: Node): Node      = this
  }
}

