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

trait Storage {
  def readArray(loc: Long): Array[Byte]
  def appendArray(arr: Array[Byte]): Long
}

// ---------------------------------------------------------

class StorageSwizzle[S <: AnyRef] {
  private var loc_i: Long = -1L
  private var value_i: S  = _

  def loc: Long = synchronized { loc_i }
  def loc_!!(x: Long) = synchronized { 
    if (loc_i >= 0L && x >= 0L)
      throw new RuntimeException("cannot override an existing swizzle loc")
    loc_i = x
    loc_i 
  }

  def value: S = synchronized { value_i }
  def value_!!(x: S) = synchronized { 
    if (x != null && value_i != null)
      throw new RuntimeException("cannot overwrite an existing swizzle value")
    value_i = x
    value_i
  }
}

