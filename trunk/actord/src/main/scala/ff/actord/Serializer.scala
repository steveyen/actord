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

import java.io._
import java.net._

trait Serializer {
  def serialize(o: AnyRef): Array[Byte]
  def deserialize(bytes: Array[Byte], offset: Int, length: Int): AnyRef
}

/**
 * See original at scala.actors.remote._ by Philipp Haller & Guy Oliver.
 */
class SSerializer(cl: ClassLoader) extends Serializer {
  def serialize(o: AnyRef): Array[Byte] = {
    try {
      val bos = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)
      out.writeObject(o)
      out.flush
      bos.toByteArray
    } catch {
      case ex => 
        println("SSerializer.serialize EXCEPTION: " + ex) // TODO: Logging.
        throw ex
    }
  }

  def deserialize(bytes: Array[Byte], offset: Int, length: Int): AnyRef = {
    val bs = new ByteArrayInputStream(bytes, offset, length)
    val is = if (cl != null)
               new SObjectInputStream(bs, cl)
             else
               new ObjectInputStream(bs)

    is.readObject
  }
}

/**
 * See original at scala.actors.remote._ by Philipp Haller & Guy Oliver.
 */
class SObjectInputStream(os: InputStream, cl: ClassLoader) extends ObjectInputStream(os) {
  override def resolveClass(cd: ObjectStreamClass): Class[T] forSome { type T } =
    try {
      cl.loadClass(cd.getName)
    } catch {
      case cnf: ClassNotFoundException =>
        super.resolveClass(cd)
    }
}

