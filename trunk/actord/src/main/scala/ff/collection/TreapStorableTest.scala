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
    ( "should be empty after create" ::
      "should handle simple appends and reads" ::
      "should handle loc append/read" ::
      Nil
    ).map(name => new SingleFileStorageTestCase(name)) :::  
    ( "should be empty after creation" ::
      Nil
    ).map(name => new TreapStorableTestCase(name)) :_*
  )
}

class SingleFileStorageTestCase(name: String) extends TestCase(name) {
  override def runTest = {
    println("test: " + name)
    name match {
      case "should be empty after create" =>
        val f = File.createTempFile("test_sfs", ".tmp")
        val s = new SingleFileStorage(f)
        val loc = s.append((loc, appender) => appender.appendUTF("hello"))
        assertEquals(loc, StorageLoc(0, 0L))

        s.close        
        f.delete
        
      case "should handle simple appends and reads" =>
        val f = File.createTempFile("test_sfs1", ".tmp")
        val s = new SingleFileStorage(f)
        val loc = s.append((loc, appender) => appender.appendUTF("hello"))
        assertEquals(loc, StorageLoc(0, 0L))
        s.close

        val s2 = new SingleFileStorage(f)
        val b2 = s2.readAt(loc, _.readUTF)
        assertEquals("hello", b2)
        s2.close

        val s3 = new SingleFileStorage(f)
        val loc3 = s3.append((loc, appender) => appender.appendUTF("world"))
        assertEquals(loc3, StorageLoc(0, 7L))
        val loc4 = s3.append((loc, appender) => appender.appendUTF("there"))
        assertEquals(loc4, StorageLoc(0, 14L))
        
        assertEquals("hello", s3.readAt(loc,  _.readUTF))
        assertEquals("world", s3.readAt(loc3, _.readUTF))
        assertEquals("there", s3.readAt(loc4, _.readUTF))
        assertEquals("world", s3.readAt(loc3, _.readUTF))
        assertEquals("there", s3.readAt(loc4, _.readUTF))
        assertEquals("hello", s3.readAt(loc,  _.readUTF))
        s3.close
        
        f.delete

      case "should handle loc append/read" =>
        val f = File.createTempFile("test_sfs2", ".tmp")
        val s = new SingleFileStorage(f)

        val loc1 = s.append((loc, appender) => appender.appendUTF("hello"))
        val loc2 = s.append((loc, appender) => appender.appendUTF("world"))
        val loc3 = s.append((loc, appender) => appender.appendUTF("there"))

        assertEquals(loc1, StorageLoc(0, 0L))
        assertEquals(loc2, StorageLoc(0, 7L))
        assertEquals(loc3, StorageLoc(0, 14L))
        
        assertEquals("hello", s.readAt(loc1, _.readUTF))
        assertEquals("world", s.readAt(loc2, _.readUTF))
        assertEquals("there", s.readAt(loc3, _.readUTF))

        val locA = s.append((loc, appender) => appender.appendLoc(loc1))
        val locB = s.append((loc, appender) => appender.appendLoc(loc2))
        val locC = s.append((loc, appender) => appender.appendLoc(loc3))        

        assertEquals(loc1, s.readAt(locA, _.readLoc))
        assertEquals(loc2, s.readAt(locB, _.readLoc))
        assertEquals(loc3, s.readAt(locC, _.readLoc))

        s.close        
        f.delete
    }
  }
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
    name match {
      case "should be empty after creation" =>
    }
  }
}
