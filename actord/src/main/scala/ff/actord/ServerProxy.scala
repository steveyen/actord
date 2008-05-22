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
package ff.actord.client

import java.io._
import java.net._

import ff.actord.Util._

abstract class MServerProxy(host: String, port: Int) 
  extends MServer {
  val s   = new Socket(host, port)
  val in  = s.getInputStream
  val out = s.getOutputStream

  def subServerList: List[MSubServer]
  
  def get(keys: Seq[String]): Iterator[MEntry]
  def set(el: MEntry, async: Boolean): Boolean
  def delete(key: String, time: Long, async: Boolean): Boolean

  /**
   * A transport protocol can convert incoming incr/decr messages to delta calls.
   */
  def delta(key: String, mod: Long, async: Boolean): Long
    
  /**
   * For add or replace.
   */
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean): Boolean

  /**
   * A transport protocol can convert incoming append/prepend messages to xpend calls.
   */
  def xpend(el: MEntry, append: Boolean, async: Boolean): Boolean

  /**
   * For CAS mutation.
   */  
  def checkAndSet(el: MEntry, cidPrev: Long, async: Boolean): String

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String]
  
  def flushAll(expTime: Long): Unit
  
  def stats: MServerStats

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry]

  def act(el: MEntry, async: Boolean): Iterator[MEntry]
}

