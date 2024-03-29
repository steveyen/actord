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

import scala.actors._
import scala.collection._

import java.io._
import java.net._
import java.util.concurrent._

import ff.actord.Util._
import ff.actord.SNode._

/*
utilizes memcached protocol.  
  allows telnet (text-based) debugging and tools.
  many client bindings already.

memcached protocol similiarities to HTTP.
  stateless.
  request-response.  
  asymmetrical roles, client-vs-server.
    because not all clients have actors or memcached-server.
  get is like HTTP GET.
    except url/query-param length is really limited.
  set is like HTTP POST.
    except set only has a boolean reply.
  the key is like URL: a name/path + optional params
    keys need to be 2-level?
      routing-part    foo/bar/baz
      optional-params ?x=1&y=2
  downsides...
    memcached protocol is non-concurrent, request-reply per connection, like http.
    cannot have many rpc's concurrently in-flight on one connection.
      is this non-concurrency a STOPPER, big perf hit?
      client-side needs to buffer pending messages?
        or, use more than one connection.
        or, use multi-get.
      especially if client-side is using react/async actors.
      need a design which allows switch to better protocol.
        in memcached, cannot push messages to client if it isn't reading.
      speaking in erlang rpc terms...
        cast becomes "set noreply"
        call becomes "call" or a "set"+"get"
        callAsync becomes "call" or a "set"+doStuff+"get"

agency API options...
  option 1, try to live with an agency API that looks like memcached API...
    agency.sendRepliGet(key, "shortMsg", replicaInfo): (badRPC | bigVal)
      => get key?shortMsg
    agency.sendRepliGetAsync(key, "shortMsg", replicaInfo): Future(badRPC | bigVal) 
      => get key?shortMsg

    agency.sendRepliMod("set", key, "shortMsg", bigVal, noReply, replicaInfo): {badRPC | Boolean}            
      => set key?shortMsg \r\n bigVal
    agency.sendRepliModAsync("set", key, "shortMsg", bigVal, noReply, replicaInfo): Future(badRPC | Boolean) 
      => set key?shortMsg \r\n bigVal

  unworkable option 2, add a special "call" protocol message for a richer API.
    the call message includes content payload on both request and response.
    not ideal because it's incompatible with memcached clients.

  unworkable option 3, simulate a "call" by with a set someKey and immediate get someKey?SESSVAR=0.
    the server-side keeps the session variables with the session.
    old memcached clients can still work.
    old memcached servers will always return empty SESSVAR. 
    does not work with UDP.
    does not work with server-router.

  option 4, simulate a "call" by with client generated uniqueSecret
    client first requests: set someKey?sideHold=uniqueSecret \r\n bigVal, with short expTime.
    and immediately requests: get someKey?sideTake=uniqueSecret
    the actord server-side keeps an expiring response map/cache (per session), with short expTime.
    current memcached clients will work correctly with actord.
    current memcached servers will return empty sideTake response.
    UDP should work.
    server-router should work, although the response map/cache gets big.
    the agency API can look more like erlang rpc, more like RemoteActor-ish
      agency.callRep(key, "shortMsg", replicaInfo, bigVal): (badRPC | bigVal)
      agency.castRep(key, "shortMsg", replicaInfo, bigVal): (badRPC | Boolean)

on server-side, actord might check key prefix to figure out whether to hook in an actor.

nodeStatus tracks up/down/suspect connection status of nodes.

actorDNodeState tracks stopped/warming/cooling/running state of ActorD nodes.

client-side needs to batch up 'get' requests.
  the 'get' train.
  multi-get(key0, key1, ...)
    splits keys by target node.
    for each target node...
      target node is controlled by a node worker.
      we add keys onto the next 'get' train.
      and registers callback actor.
      a default callback actor react loop is optionally provided.
        the default react actor receives msg when a node worker receives an END.
        has a countdown when all node workers have returned or errored/timed out.

node worker is a thread.
  there is a FIFO queue of pending requests to be sent.
    a queue item has listeners interested in response values.
    the listeners are actors.
  get msgs go onto an unsent 'get' bucket.
    don't want dirty reads, or set-set-set-get-set reordering for a given thread.
    for a given thread, we also cannot reorder setAsync-setAsync-setAsync-getAsync-setAsync.
    but, given the above queue, other threads sending their first request
      can piggy-back onto the earliest unsent get/getAsync.
    so, for a given thread, we remember the last queued request.
      and if the thread's next request is a get/getAsync,
      we can scan the list from the last queued request instead
      to look for a piggy-back opportunity, 
      instead of just appending a brand new queue item.
      will this be faster/better?
        more complex on the client side. 
          perhaps more client-side locking / larger sync blocks.
        multi-gets are larger?
        more server-side buffering?
      perhaps just build the system with piggy-back'ing in mind, but do it later.
  responses are bucketed by the node worker,
    so that listeners don't have to do the mux/demux bookkeeping.
    each VALUE response is bucketed into a listener's reply list.
    two listeners might have requested the same key,
      so the VALUE response could go onto >1 reply list.
    when we get an END, the listeners are sent their reply lists.

agency
  tracks node / cluster hierarchical map and node state

someplace that tracks node status and knows about reconnection
  node status 
  this can be refactored in later
  tracks lost node connections, timestamps

nodeManager / nodeWorker
  tracks connected nodes
  tracks pending messages
  nodeWorkers = [Node, NodeWorker]
*/

case class Node(host: String, port: Int)

trait NodeManager {
  protected val workers = new mutable.HashMap[Node, NodeWorker]

  def workerFor(n: Node): NodeWorker = 
    if (n != null) {
      val w = synchronized { workers.get(n) }
      if (w.isDefined)
          w.get
      else {
        // TODO: Although we're creating the node worker outside synchronized, 
        // we might have a stampede of wasted new workers or connections 
        // to a node, because only one will win.
        //
        // TODO: Allow for multiple workers per node.
        //
        val workerNew = createNodeWorker(n)

        synchronized {
          val w = workers.get(n)
          if (w.isEmpty) {
            workers += (n -> workerNew)
            workerNew
          } else {
            workerNew.close
            workerNew.pend(NOOP)
            w.get
          }
        }
      }
    } else
      null

  def workerDone(w: NodeWorker): Unit = synchronized { 
    if (w != null &&
        w.manager == this)
      workers.get(w.node).
              foreach(curr => {
                if (curr == w)      // Make sure we're removing w, not some 
                  workers -= w.node // other worker that popped up concrrently.
              })
  }

  def createNodeWorker(n: Node): NodeWorker // Throws exception if cannot connect.

  def serializer: Serializer

  val NOOP = new PendingRequest(Nil, null, null, null, 0L) // Sent to help workers wakeup.
}

abstract class NodeWorker(val manager: NodeManager, val node: Node) {
  def alive: Boolean
  def close: Unit
  def transmit(callee: Card, msgArr: Array[Byte]): Unit

  protected val pendingRequests: BlockingQueue[PendingRequest] = 
    createPendingRequestsQueue

  def createPendingRequestsQueue: BlockingQueue[PendingRequest] = 
    new ArrayBlockingQueue[PendingRequest](60)

  def pend(caller: Actor, callee: Card, msg: AnyRef): Unit = {
    val pr = new PendingRequest(caller, callee, msg, 
                                manager.serializer.serialize(msg))
    if (alive)
      pend(pr)
    else
      pr.failure("could not send message via dead node worker: " + node)
  }

  def pend(pr: PendingRequest): Unit =
    pendingRequests.put(pr)

  def run {
    try {
      while (alive) {
        val pr = pendingRequests.take
        if (alive && 
            pr != null &&
            pr != manager.NOOP) 
          try {
            transmit(pr.callee, pr.msgArr)
          } catch {
            case ex => 
              pr.failure(ex)
              println("transmit failure to node: " + node)
          }
      }
    } finally {
      runDone
    }
  }

  def runDone: Unit = {
    manager.workerDone(this)

    val reason = "could not send message - node worker done: " + node

    while (!pendingRequests.isEmpty) {
      val pr = pendingRequests.take
      if (pr != null)
          pr.failure(reason)
    }
  }
}

class PendingRequest(protected var callbacks_i: List[Actor], 
                     val callee: Card, 
                     val msg: AnyRef, 
                     val msgArr: Array[Byte],
                     val pendTime: Long) {
  def this(callback: Actor, callee: Card, msg: AnyRef, msgArr: Array[Byte]) = 
      this(List(callback), callee, msg, msgArr, System.currentTimeMillis)

  def callbacks = synchronized { callbacks_i }

  def addCallback(a: Actor): Unit = synchronized {
    if (a != null)
        callbacks_i = a :: callbacks_i
  }

  def failure(failReason: AnyRef): Unit = 
    callbacks.foreach(_ ! Failure(callee, msg, failReason))
}

// ----------------------------------------------

class SNodeManager extends NodeManager {
  def createNodeWorker(n: Node): NodeWorker = {
    val w = new SNodeWorker(this, n)
    (new Thread(w)).start
    w
  }

  val serializer: Serializer = createSerializer

  def createSerializer: Serializer = new SSerializer(null)
}

class SNodeWorker(override val manager: NodeManager, override val node: Node) 
  extends NodeWorker(manager: NodeManager, node) with Runnable {
  protected val s = new Socket(node.host, node.port)

  s.setTcpNoDelay(true)

  def alive: Boolean = synchronized { s.isConnected && !s.isClosed }
  def close: Unit    = synchronized { s.close }

  protected var bs = new BufferedOutputStream(s.getOutputStream)

  def write(m: String): Unit                                = bs.write(stringToArray(m))
  def write(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def write(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def flush: Unit = 
    try {
      bs.flush
    } catch {
      case ex => close; throw ex
    }

  def transmit(callee: Card, msgArr: Array[Byte]): Unit = synchronized {
    try {
      import SNode._

      write(setBytes)      // Emit memcached protocol "set" command,
      write(callee.base)   // but with key generated from callee card.
      write(moreMarkBytes)
      write(callee.more)
      write(setFlagBytes)
      write(String.valueOf(msgArr.length))
      write(noReplyBytes)
      write(CRNLBytes)
      write(msgArr)
      write(CRNLBytes)
      flush
    } catch {
      case ex =>
        println("transmit failure to: " + node + " alive: " + alive)
        throw ex
    }
  }

  val watcher = new Thread() {
    override def run {
      try {
        // We're not expecting anything to read, so this should
        // block until there's an error, the socket closed, etc.
        //
        s.getInputStream.read 
      } finally {
        close
        manager.workerDone(SNodeWorker.this)
        pend(manager.NOOP)
      }
    }
  }

  watcher.start
}

object SNode {
  val dispatchMark  = "_aD|" // Tells us it's an actor dispatch message.
  val moreMark      = "?"
  val moreMarkBytes = stringToArray(moreMark)
  val setBytes      = stringToArray("set " + dispatchMark)
  val setFlagBytes  = stringToArray(" 0 0 ")
  val noReplyBytes  = stringToArray(" noreply")

  def cardToEntryKey(c: Card): String = 
    dispatchMark + c.base + moreMark + c.more

  def debugln(m: String): Unit = println(m)
}

// Listens on the given port for memcached-speaking client connections,
// but also understands the extended Agency-related memcached-protocol 
// semantics to dispatch messages to local actors.
//
class SReceptionist(host: String, port: Int, agency: Agency, serializer: Serializer) {
  val m = new MainProgSimple() {
    override def createServer(numProcessors: Int, limitMem: Long): MServer = {
      var pool: ActorPool   = null
      val zero: Array[Byte] = new Array[Byte](0)

      val server: MServer = new MMainServer(numProcessors, limitMem) {
        override def set(el: MEntry, async: Boolean): Boolean = {
          // See if we should dispatch as a msg to a local actor.
          //
          if (el.key.startsWith(SNode.dispatchMark)) {
            val msg = serializer.deserialize(el.data, 0, el.data.length)
            val beg = SNode.dispatchMark.length
            val end = el.key.length
            var moreMarkAt = el.key.indexOf(SNode.moreMark, beg)
            if (moreMarkAt < beg)
                moreMarkAt = end

            val calleeBase = el.key.substring(beg, moreMarkAt)
            val calleeMore = el.key.substring(Math.min(moreMarkAt + 1, end), end)
            val callee = Card(calleeBase, calleeMore)

            val localA = agency.localActorFor(callee)
            if (localA.isDefined) { // One reason why we get here is handling a Reply.
                localA.get ! msg
                return true
            } else if (callee.more == Agency.factoryCard.more) {
              // Invoke our local actor that can create more local actors, aka the 
              // factory actor), if any.  The factory actor might add any new 
              // actors into the fluid actor pool.
              //
              val factory = agency.localActorFor(Agency.factoryCard)
              if (factory.isDefined) {
                  factory.get ! CreateActor(callee, msg, pool)
                  return true
              }
            } else {
              // Invoke an actor that's in the fluid actor pool (stored in the MServer),
              // that probably got into the pool originally by the LATCCMLA above.
              //
              val iter = this.get(List(el.key))
              for (e <- iter) {
                val att = e.attachment
                if (att != null &&
                    att.isInstanceOf[Actor]) {
                    att.asInstanceOf[Actor] ! msg
                    return true
                }
              }
            }
            return false
          }

          // Otherwise, just be a normal memcached "set" command.
          //
          super.set(el, async)
        }
      }

      // Using var instead of val idiom to prevent scalac circular definition complaint.
      //
      pool = new ActorPool() { 
        def offer(card: Card, actor: Actor, expiration: Long, async: Boolean): Boolean = 
            offer(SNode.cardToEntryKey(card), zero, expiration, actor, async)

        def offer(key: String, data: Array[Byte], expiration: Long, attachment: AnyRef, async: Boolean): Boolean = {
          val el = MEntry(key, 0, expiration, data, 0)
          if (el != null) {
              el.attachment_!(attachment)
              server.addRep(el, true, async)
          } else
              false
        }
      }

      server
    }
  }

  m.start(startArg _)

  def startArg(arg: String, defaultValue: String): String =
    arg match {
      case "portTCP" => port.toString
      case _         => defaultValue
    }
}
