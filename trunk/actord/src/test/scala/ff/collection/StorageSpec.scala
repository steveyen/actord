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

import org.specs._
import org.specs.runner._

import scala.collection._

import java.io._

class  StorageSpecTest   extends JUnit3(StorageSpec)
object StorageSpecRunner extends ConsoleRunner(StorageSpec)
object StorageSpec extends Specification {
  "FileStorage" should {
    "should be empty after create" in {
      val f = File.createTempFile("test_sfs", ".tmp")
      val s = new FileStorage(f)
      val loc = s.append((loc, appender) => appender.appendUTF("hello"))
      loc must_== StorageLoc(0, 0L)

      s.close        
      f.delete
    }
        
    "should handle simple appends and reads" in {
      val f = File.createTempFile("test_sfs1", ".tmp")
      val s = new FileStorage(f)
      val loc = s.append((loc, appender) => appender.appendUTF("hello"))
      loc must_== StorageLoc(0, 0L)
      s.close

      val s2 = new FileStorage(f)
      val b2 = s2.readAt(loc, _.readUTF)
      "hello" must_== b2
      s2.close

      val s3 = new FileStorage(f)
      val loc3 = s3.append((loc, appender) => appender.appendUTF("world"))
      loc3 must_== StorageLoc(0, 7L)
      val loc4 = s3.append((loc, appender) => appender.appendUTF("there"))
      loc4 must_== StorageLoc(0, 14L)
      
      "hello" must_== s3.readAt(loc,  _.readUTF)
      "world" must_== s3.readAt(loc3, _.readUTF)
      "there" must_== s3.readAt(loc4, _.readUTF)
      "world" must_== s3.readAt(loc3, _.readUTF)
      "there" must_== s3.readAt(loc4, _.readUTF)
      "hello" must_== s3.readAt(loc,  _.readUTF)
      s3.close
      
      f.delete
    }

    "should handle loc append/read" in {
      val f = File.createTempFile("test_sfs2", ".tmp")
      val s = new FileStorage(f)

      val loc1 = s.append((loc, appender) => appender.appendUTF("hello"))
      val loc2 = s.append((loc, appender) => appender.appendUTF("world"))
      val loc3 = s.append((loc, appender) => appender.appendUTF("there"))

      loc1 must_== StorageLoc(0, 0L)
      loc2 must_== StorageLoc(0, 7L)
      loc3 must_== StorageLoc(0, 14L)
      
      "hello" must_== s.readAt(loc1, _.readUTF)
      "world" must_== s.readAt(loc2, _.readUTF)
      "there" must_== s.readAt(loc3, _.readUTF)

      val locA = s.append((loc, appender) => appender.appendLoc(loc1))
      val locB = s.append((loc, appender) => appender.appendLoc(loc2))
      val locC = s.append((loc, appender) => appender.appendLoc(loc3))        

      loc1 must_== s.readAt(locA, _.readLoc)
      loc2 must_== s.readAt(locB, _.readLoc)
      loc3 must_== s.readAt(locC, _.readLoc)

      s.close        
      
      val s2 = new FileStorage(f)
      loc1 must_== s2.readAt(locA, _.readLoc)
      loc2 must_== s2.readAt(locB, _.readLoc)
      loc3 must_== s2.readAt(locC, _.readLoc)
      s2.close

      f.delete
    }

    "should handle byte append/read" in {
      val f = File.createTempFile("test_sfs3", ".tmp")
      val s = new FileStorage(f)

      val loc1 = s.append((loc, appender) => appender.appendArray("hello".getBytes, 0, 5))
      val loc2 = s.append((loc, appender) => appender.appendArray("world".getBytes, 0, 5))
      val loc3 = s.append((loc, appender) => appender.appendArray("there".getBytes, 0, 5))

      loc1 must_== StorageLoc(0, 0L)
      loc2 must_== StorageLoc(0, 9L)
      loc3 must_== StorageLoc(0, 18L)
      
      "hello" must_== new String(s.readAt(loc1, _.readArray))
      "world" must_== new String(s.readAt(loc2, _.readArray))
      "there" must_== new String(s.readAt(loc3, _.readArray))

      s.close        

      val s2 = new FileStorage(f)
      "hello" must_== new String(s2.readAt(loc1, _.readArray))
      "world" must_== new String(s2.readAt(loc2, _.readArray))
      "there" must_== new String(s2.readAt(loc3, _.readArray))

      val loc1b = s2.append((loc, appender) => appender.appendArray("11111".getBytes, 0, 5))
      val loc2b = s2.append((loc, appender) => appender.appendArray("22222".getBytes, 0, 5))
      val loc3b = s2.append((loc, appender) => appender.appendArray("33333".getBytes, 0, 5))

      loc1b must_== StorageLoc(0, 27L)
      loc2b must_== StorageLoc(0, 36L)
      loc3b must_== StorageLoc(0, 45L)
      
      "hello" must_== new String(s2.readAt(loc1, _.readArray))
      "world" must_== new String(s2.readAt(loc2, _.readArray))
      "there" must_== new String(s2.readAt(loc3, _.readArray))

      "11111" must_== new String(s2.readAt(loc1b, _.readArray))
      "22222" must_== new String(s2.readAt(loc2b, _.readArray))
      "33333" must_== new String(s2.readAt(loc3b, _.readArray))

      s2.close

      f.delete
    }
  }
}
