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
  final val ZERO      = java.lang.Integer.valueOf(0)
  final val SPACE     = ' '.asInstanceOf[Byte]
  final val CR        = '\r'.asInstanceOf[Byte]
  final val NL        = '\n'.asInstanceOf[Byte]
  final val CRNL      = "\r\n"
  final val CRNLBytes = CRNL.getBytes

  def nowInSeconds: Long = System.currentTimeMillis / 1000
  
  def itemToLong(items: Seq[String], at: Int) =
    if (items.length > at)
      parseLong(items(at), 0L)
    else 
      0L
      
  def parseLong(s: String, defaultVal: Long) = try { 
    s.trim.toLong 
  } catch { 
    case _ => defaultVal
  }
}

