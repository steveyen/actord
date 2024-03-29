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

import ff.actord.Util._

/**
 * The MServerProxy is a simple proxy to a remote memcached/actord server (running at host:port).
 * Its 'local' interface is just MServer, and it's an interesting exercise in using the
 * MNetworkReader implementation on the client-side.  But, it also displays some of the 
 * famous fallacies of distributed computing, such as not surfacing timeouts, etc, in the API.
 * Meant for single-threaded usage.
 */
class MServerProxy(s: Socket) extends MServer {
  def this(host: String, port: Int) = this(new Socket(host, port))

  def initSocket(s: Socket): Unit = s.setTcpNoDelay(true)

  initSocket(s)

  def subServerList: List[MSubServer] = Nil
  
  protected var is = s.getInputStream
  protected var os = s.getOutputStream
  protected var bs = new BufferedOutputStream(os)

  def write(m: String): Unit                                = bs.write(stringToArray(m))
  def write(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def write(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def isClosed = s == null || s.isClosed

  def close: Unit = {
    if (s != null)
        s.close
    is = null
    os = null
    bs = null
  }

  def flush: Unit = 
    try {
      bs.flush
    } catch {
      case ex @ _ => close; throw ex
    }

  /**
   * Processes the response/reply from the server.
   */
  class Response(protocol: MProtocol) extends MNetworkReader with MSession { 
    def connRead(buf: Array[Byte], offset: Int, length: Int): Int = 
      try {
        is.read(buf, offset, length)
      } catch {
        case ex @ _ => close; MServerProxy.this.close; throw ex
      }

    def connClose: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def messageProcess(cmdArr: Array[Byte], cmdLen: Int, available: Int): Int = 
      protocol.process(this, cmdArr, cmdLen, available)

    def ident: Long = 0L
    def close: Unit = { end = true } // Overriding the meaning of 'close' for the proxy/client-side.

    def write(bytes: Array[Byte], offset: Int, length: Int): Unit = 
      println(arrayToString(bytes, offset, length)) // TODO: Log these.

    var end = false
    def go  = while (!end) messageRead
  }

  def responseValues: Iterator[MEntry] = {
    var xs: List[MEntry] = Nil
    new Response(new MProtocol {
      override def specs = List(
        new MSpec("END", (cmd) => { cmd.session.close }),
        new MSpec("VALUE <key> <flags> <dataSize> [cid]", (cmd) => { xs = cmd.entry :: xs }) {
          override def casParse(cmdArgs: Seq[String]) = itemToLong(cmdArgs, pos_cas, 0L)
        })
    }).go
    xs.elements
  }

  def response(good: String, fail: String, async: Boolean): Boolean = {
    var result = true
    if (!async) 
      new Response(new MProtocol {
        override def specs = List(
          MSpec(good, (cmd) => { result = true;  cmd.session.close }),
          MSpec(fail, (cmd) => { result = false; cmd.session.close }))
      }).go
    result
  }

  val getBytes      = stringToArray("get ")
  val getsBytes     = stringToArray("gets ")
  val setBytes      = stringToArray("set ")
  val deleteBytes   = stringToArray("delete ")
  val incrBytes     = stringToArray("incr ")
  val decrBytes     = stringToArray("decr ")
  val addBytes      = stringToArray("add ")
  val replaceBytes  = stringToArray("replace ")
  val appendBytes   = stringToArray("append ")
  val prependBytes  = stringToArray("prepend ")
  val casBytes      = stringToArray("cas ")
  val rangeBytes    = stringToArray("range ")
  val actBytes      = stringToArray("act ")
  val flushAllBytes = stringToArray("flush_all ")
  val noreplyBytes  = stringToArray(" noreply")

  def get(keys: Seq[String]): Iterator[MEntry] = { // We send a gets, instead of a get, in order
    write(getsBytes)                               // to receive full CAS information.
    write(keys.mkString(" "))
    write(CRNLBytes)
    flush
    responseValues
  }

  def writeNoReplyFlag(async: Boolean): Unit =
    if (async)
      write(noreplyBytes)

  def set(el: MEntry, async: Boolean): Boolean = {
    write(setBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  def delete(key: String, time: Long, async: Boolean): Boolean = {
    write(deleteBytes)
    write(key + " " + time)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    flush
    response("DELETED", "NOT_FOUND", async)
  }

  /**
   * A transport protocol can convert incoming incr/decr messages to delta calls.
   */
  def delta(key: String, mod: Long, async: Boolean): Long = {
    if (mod > 0L) 
      write(incrBytes)
    else 
      write(decrBytes)
    write(key + " " + Math.abs(mod))
    writeNoReplyFlag(async)
    write(CRNLBytes)
    flush

    var result = -1L
    if (!async) 
      new Response(new MProtocol {
        override def findSpec(x: Array[Byte], xLen: Int): MSpec = 
          MSpec("UNUSED", (cmd) => { // Always returns this MSpec to process incr/decr response.
            val s = arrayToString(x, 0, xLen)
            if (s != "NOT_FOUND")
              result = parseLong(s, -1L)
            cmd.session.close 
          })
      }).go
    result
  }
    
  /**
   * For add or replace.
   */
  def addRep(el: MEntry, isAdd: Boolean, async: Boolean): Boolean = {
    if (isAdd) 
      write(addBytes)
    else
      write(replaceBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * A transport protocol can convert incoming append/prepend messages to xpend calls.
   */
  def xpend(el: MEntry, append: Boolean, async: Boolean): Boolean = {
    if (append) 
      write(appendBytes)
    else
      write(prependBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    response("STORED", "NOT_STORED", async)
  }

  /**
   * For CAS mutation.
   */  
  def checkAndSet(el: MEntry, cid: Long, async: Boolean): String = {
    write(casBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length + " " + cid)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush

    var result = ""
    if (!async) 
      new Response(new MProtocol {
        override def specs = List(
          MSpec("STORED",    (cmd) => { result = "STORED";    cmd.session.close }),
          MSpec("EXISTS",    (cmd) => { result = "EXISTS";    cmd.session.close }),
          MSpec("NOT_FOUND", (cmd) => { result = "NOT_FOUND"; cmd.session.close }))
      }).go
    result
  }

  /**
   * The keys in the returned Iterator are unsorted.
   */
  def keys: Iterator[String] = Nil.elements
  
  def flushAll(expTime: Long): Unit = {
    write(flushAllBytes)
    write(expTime.toString)
    write(CRNLBytes)
    flush
    response("OK", "NOT_OK", false)
  }
  
  def stats: MServerStats = null

  /**
   * The keyFrom is the range's lower-bound, inclusive.
   * The keyTo is the range's upper-bound, exclusive.
   */
  def range(keyFrom: String, keyTo: String): Iterator[MEntry] = {
    write(rangeBytes)
    write(keyFrom)
    write(SPACEBytes)
    write(keyTo)
    write(CRNLBytes)
    flush
    responseValues
  }

  def act(el: MEntry, async: Boolean): Iterator[MEntry] = {
    write(actBytes)
    write(el.key + " " + el.flags + " " + el.expTime + " " + el.data.length)
    writeNoReplyFlag(async)
    write(CRNLBytes)
    write(el.data)
    write(CRNLBytes)
    flush
    responseValues
  }
}

