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

/**
 * Utilities that are independent of server or client.
 */
object Util {
  final val ZERO       = java.lang.Integer.valueOf(0)
  final val SPACE      = ' '.asInstanceOf[Byte]
  final val SPACEBytes = stringToArray(" ")
  final val CR         = '\r'.asInstanceOf[Byte]
  final val NL         = '\n'.asInstanceOf[Byte]
  final val CRNL       = "\r\n"
  final val CRNLBytes  = stringToArray(CRNL)

  implicit def ostringWrapper(x: String) = OString(x)
  implicit def ostringUnwrapper(x: OString): String = x.mkString

  def nowInSeconds: Long = System.currentTimeMillis / 1000
  
  def parseLong(s: String, defaultVal: Long) = try { 
    java.lang.Long.parseLong(s.trim)
  } catch { 
    case _ => defaultVal
  }

  def itemToInt(items: Seq[String], at: Int): Int = itemToInt(items, at, 0)
  def itemToInt(items: Seq[String], at: Int, defVal: Int): Int =
    if (at >= 0 && 
        at < items.length) 
      Integer.parseInt(items(at))
    else 
      defVal

  def itemToLong(items: Seq[String], at: Int): Long = itemToLong(items, at, 0L)
  def itemToLong(items: Seq[String], at: Int, defVal: Long): Long =
    if (at >= 0 && 
        at < items.length) 
      parseLong(items(at), defVal)
    else 
      defVal

  def stringToArray(s: String): Array[Byte] = stringToArray(s, 0, s.length)
  def stringToArray(s: String, offset: Int, length: Int): Array[Byte] = {
    val r = new Array[Byte](length)
    s.getBytes(offset, offset + length, r, 0)
    r
  }

  def arrayToString(a: Array[Byte]): String = arrayToString(a, 0, a.length)
  def arrayToString(a: Array[Byte], offset: Int, length: Int): String = new String(a, 0, offset, length)

  def arraySlice[T](a: Array[T], offset: Int, length: Int): Array[T] = {
    val dest = new Array[T](length)
    Array.copy(a, offset, dest, 0, length)
    dest
  }

  /**
   * For example, arrayParsePositiveInt("1234".getBytes, 0, 4) == 1234.
   * Does not work on negatives.
   */
  def arrayParsePositiveInt(a: Array[Byte], offset: Int, length: Int): Int = { 
    var x: Int = a(offset).toInt - '0'
    var i: Int = offset + 1
    val j: Int = offset + length
    while (i < j) {
      x = (x * 10) + (a(i).toInt - '0')
      i += 1
    }
    x
  }

  def arrayEnsureSize[T](a: Array[T], length: Int): Array[T] = {
    if (length <= a.length)
      a
    else {
      val x = new Array[T](length * 4)
      Array.copy(a, 0, x, 0, a.length)
      x
    }
  }

  def arraySplit(a: Array[Byte], offset: Int, len: Int, x: Byte): Seq[String] = { 
    var r = new Array[String](10) // Faster than mutable.ArrayBuffer.
    var k = 0                     // The next position in r to write.
    var s = offset
    var i = offset
    val j = offset + len
    while (i < j) { 
      if (a(i) == x) { // Faster than regexp-based String.split() method.
        if (s < i) {
          r = arrayEnsureSize(r, k + 1)
          r(k) = arrayToString(a, s, i - s)
          k += 1
        }
        s = i + 1
      }
      i += 1
    }
    if (s < i) {
      r = arrayEnsureSize(r, k + 1)
      r(k) = arrayToString(a, s, i - s)
      k += 1
    }

    arraySlice(r, 0, k)
  }

  def arrayIndexOf(a: Array[Byte], offset: Int, length: Int, x: Byte): Int = { 
    var i = offset
    val j = offset + length
    while (i < j) { // Faster than scala's iterator-based indexOf() implementation.
      if (a(i) == x)
        return i
      i += 1
    }
    -1
  }

  /**
   * Find the n'th index of a given search value x in an array.
   * The n'th parameter is 1-based.  
   */
  def arrayNthIndexOf(a: Array[Byte], offset: Int, length: Int, x: Byte, nth: Int): Int = {
    var n = nth
    var i = offset
    val j = offset + length
    while (i < j) {
      if (a(i) == x) {
        n = n - 1
        if (n <= 0)
          return i
      }
      i += 1
    }
    -1
  }

  def arrayCompare(a: Array[Byte], b: Array[Byte]): Int = 
      arrayCompare(a, 0, a.length, b, 0, b.length)

  def arrayCompare(a: Array[Byte], aLength: Int, b: Array[Byte], bLength: Int): Int = 
      arrayCompare(a, 0, aLength, b, 0, bLength)

  def arrayCompare(a: Array[Byte], aOffset: Int, aLength: Int, 
                   b: Array[Byte], bOffset: Int, bLength: Int): Int = { // Like memcmp.
    val len = if (aLength < bLength) aLength else bLength
    var ii = 0
    var ia = aOffset
    var ib = bOffset
    while (ii < len) {
      val c = a(ia) - b(ib)
      if (c != 0)
         return c
      ii += 1
      ia += 1
      ib += 1
    }
    aLength - bLength
  }

  def arrayStartsWith(a: Array[Byte], aLength: Int, prefix: Array[Byte]): Boolean = 
    if (aLength > prefix.length)
      arrayCompare(prefix, 0, prefix.length, a, 0, prefix.length) == 0
    else
      false

  def arrayEndsWith(a: Array[Byte], aLength: Int, suffix: Array[Byte]): Boolean = 
    if (aLength > suffix.length)
      arrayCompare(suffix, 0, suffix.length, a, aLength - suffix.length, suffix.length) == 0
    else
      false

  def arrayHash(a: Array[Byte]): Int = {
    val F32_INIT  = 2166136261L // Simple hashCode doesn't work on Array[Byte].
    val F32_PRIME = 16777619
    val len = a.length
    var r = F32_INIT
    var i = 0
    while (i < len) {
      r *= F32_PRIME;
      r ^= a(i)
      i += 1
    }
    (r & 0xffffffffL).toInt
  }
}

