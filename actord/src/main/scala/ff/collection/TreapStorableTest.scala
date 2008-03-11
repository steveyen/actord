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

import scala.testing.SUnit
import scala.testing.SUnit._

import java.io._

/**
 * Tests persistent treap storage.
 */
class TreapStorableTest extends TestConsoleMain {
  def suite = new TestSuite(
    ( "should be empty after creation" ::
      Nil
    ).map(name => new TreapStorableTestCase(name)) :_*
  )
}

class TreapStorableTestCase(name: String) extends TestCase(name) {
  val empty = TreapEmptyNode[String, String]
  
  class TS(override val root: TreapNode[String, String],
           override val io: Storage)
    extends TreapStorable[String, String](root, io) {
    def serializeKey(x: String): Array[Byte]     = x.getBytes
    def unserializeKey(arr: Array[Byte]): String = new String(arr)
  
    def serializeValue(x: String): Array[Byte]     = x.getBytes
    def unserializeValue(arr: Array[Byte]): String = new String(arr)
  }
  
  override def runTest = {
    println("test: " + name)

    val f = File.createTempFile("test_treapstorable", ".tmp")
    val s = new SingleFileStorage(f)
    val t = new TS(empty, s)

    try {
      name match {
        case "should be empty after creation" =>
      }
    } finally {
      s.close        
      f.delete
    }
  }
}
