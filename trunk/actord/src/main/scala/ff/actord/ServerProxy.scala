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
  def subServerList: List[MSubServer] = Nil
  
  val s  = new Socket(host, port)
  val is = s.getInputStream
  val os = s.getOutputStream

  def write(m: String): Unit = os.write(stringToArray(m))

  class Response(protocol: MProtocol) extends MNetworkReader with MSession { // Processes the response/reply from the server.
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = is.read(buf, offset, length)
    def connClose: Unit = { /* NO-OP */ }

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      protocol.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = { /* NO-OP */ } // TODO: Log these.

    var end = false
    def go  = while (!end) messageRead
  }

  def get(keys: Seq[String]): Iterator[MEntry] = {
    write("get " + keys.mkString(" ") + CRNL)
    os.flush

    var xs: List[MEntry] = Nil
    new Response(new MProtocol {
      override def singleLineSpecs = List(MSpec("END",                         (cmd) => { cmd.session.close }))
      override def  multiLineSpecs = List(MSpec("VALUE <key> <flags> <bytes>", (cmd) => { xs = cmd.entry :: xs }))
    }).go
    xs.elements
  }

  def set(el: MEntry, async: Boolean): Boolean = {
    write("set " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
          (if (async) " noreply" else "") + CRNL)
    os.write(el.data, 0, el.data.length)
    os.write(CRNLBytes)
    os.flush

    var result = true
    if (!async) 
      new Response(new MProtocol {
        override def singleLineSpecs = List(
          MSpec("STORED",     (cmd) => { result = true;  cmd.session.close }),
          MSpec("NOT_STORED", (cmd) => { result = false; cmd.session.close }))
      }).go
    result
  }

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

