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

class MServerProxy(host: String, port: Int) 
  extends MServer {
  def subServerList: List[MSubServer] = Nil
  
  val s  = new Socket(host, port)
  val is = s.getInputStream
  val os = s.getOutputStream
  val bs = new BufferedOutputStream(os)

  def write(m: String): Unit = bs.write(stringToArray(m))

  class Response(protocol: MProtocol) extends MNetworkReader with MSession { // Processes the response/reply from the server.
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = is.read(buf, offset, length)
    def connClose: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      protocol.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = { /* NO-OP */ } // TODO: Log these.

    var end = false
    def go  = while (!end) messageRead
  }

  def responseValues: Iterator[MEntry] = {
    var xs: List[MEntry] = Nil
    new Response(new MProtocol {
      override def singleLineSpecs = List(MSpec("END",                         (cmd) => { cmd.session.close }))
      override def  multiLineSpecs = List(MSpec("VALUE <key> <flags> <bytes>", (cmd) => { xs = cmd.entry :: xs }))
    }).go
    xs.elements
  }

  def response(good: String, fail: String, async: Boolean): Boolean = {
    var result = true
    if (!async) 
      new Response(new MProtocol {
        override def singleLineSpecs = List(
          MSpec(good, (cmd) => { result = true;  cmd.session.close }),
          MSpec(fail, (cmd) => { result = false; cmd.session.close }))
      }).go
    result
  }

  def get(keys: Seq[String]): Iterator[MEntry] = {
    write("get " + keys.mkString(" ") + CRNL)
    bs.flush
    responseValues
  }

  def set(el: MEntry, async: Boolean): Boolean = {
    write("set " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
          (if (async) " noreply" else "") + CRNL)
    bs.write(el.data, 0, el.data.length)
    bs.write(CRNLBytes)
    bs.flush
    response("STORED", "NOT_STORED", async)
  }

  def delete(key: String, time: Long, async: Boolean): Boolean = {
    write("delete " + key + " " + time + 
          (if (async) " noreply" else "") + CRNL)
    bs.flush
    response("DELETED", "NOT_FOUND", async)
  }

  /**
   * A transport protocol can convert incoming incr/decr messages to delta calls.
   */
  def delta(key: String, mod: Long, async: Boolean): Long = {
    if (mod > 0L) 
      write("incr " + key + " " + mod + (if (async) " noreply" else "") + CRNL)
    else
      write("decr " + key + " " + (-mod) + (if (async) " noreply" else "") + CRNL)
    bs.flush

    var result = 0L
    if (!async) 
      new Response(new MProtocol {
        override def findSpec(x: Array[Byte], xLen: Int, lookup: Array[List[MSpec]]): Option[MSpec] = 
          Some(MSpec("UNUSED", (cmd) => { 
            val s = arrayToString(x, 0, xLen)
            if (s != "NOT_FOUND")
              result = parseLong(s, 0L)
            cmd.session.close 
          }))
      }).go
    result
  }
    
  /**
   * For add or replace.
   */
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean): Boolean = {
    if (isAdd) 
      write("add " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
            (if (async) " noreply" else "") + CRNL)
    else
      write("replace " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
            (if (async) " noreply" else "") + CRNL)
    bs.write(el.data, 0, el.data.length)
    bs.write(CRNLBytes)
    bs.flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * A transport protocol can convert incoming append/prepend messages to xpend calls.
   */
  def xpend(el: MEntry, append: Boolean, async: Boolean): Boolean = {
    if (append) 
      write("append " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
            (if (async) " noreply" else "") + CRNL)
    else
      write("prepend " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + 
            (if (async) " noreply" else "") + CRNL)
    bs.write(el.data, 0, el.data.length)
    bs.write(CRNLBytes)
    bs.flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * For CAS mutation.
   */  
  def checkAndSet(el: MEntry, cid: Long, async: Boolean): String = {
    write("cas " + el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + " " + cid + 
          (if (async) " noreply" else "") + CRNL)
    bs.write(el.data, 0, el.data.length)
    bs.write(CRNLBytes)
    bs.flush

    var result = ""
    if (!async) 
      new Response(new MProtocol {
        override def singleLineSpecs = List(
          MSpec("STORED",     (cmd) => { result = "STORED";    cmd.session.close }),
          MSpec("EXISTS",     (cmd) => { result = "EXISTS";    cmd.session.close }),
          MSpec("NOT_STORED", (cmd) => { result = "NOT_FOUND"; cmd.session.close }))
      }).go
    result
  }

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String] = Nil.elements
  
  def flushAll(expTime: Long): Unit = { /* TODO */ }
  
  def stats: MServerStats = null

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = Nil.elements

  def act(el: MEntry, async: Boolean): Iterator[MEntry] = Nil.elements
}

