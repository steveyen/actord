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

nodeManager
  tracks inflight and pending messages
  nodeWorkers = [Node, NodeWorker]
*/

case class Card(base: String, more: String) {
  def ~> (msg: AnyRef): Unit = Agency.default.pend(Actor.self, this, msg)
}

object Agency {
  val default = new ActorAgency
}

trait Agency {
  def pend(caller: Actor, callee: Card, msg: AnyRef): Unit = 
      pend(cardFor(caller), callee, msg)

  def pend(caller: Card, callee: Card, msg: AnyRef): Unit

  protected val localCards  = new mutable.HashMap[Card, Actor] 
  protected val localActors = new mutable.HashMap[Actor, Card] 

  def localRegister(c: Card, a: Actor): Unit = synchronized {
    localActors.get(a).foreach(localUnregister _)
    localCards  += (c -> a)
    localActors += {a -> c}
  }

  def localUnregister(c: Card): Unit = synchronized {
    localCards.get(c).foreach(a => localActors -= a)
    localCards  -= c
  }

  def localLookup(c: Card): Option[Actor] = synchronized { localCards.get(c) }

  def cardFor(localActor: Actor): Card = synchronized { 
    localActors.getOrElseUpdate(localActor, {
      nextCard += 1
      Card("memcached://127.0.0.1:11211", nextCard.toString)
    })
  }

  protected var nextCard: Long = 0L
}

case class Frame   (caller: Card, callee: Card, msg: AnyRef)
case class Reply   (callee: Card, originalMsg: AnyRef, reply: AnyRef)
case class Failure (callee: Card, originalMsg: AnyRef, failReason: AnyRef)

class ActorAgency extends Agency {
  def pend(caller: Card, callee: Card, msg: AnyRef): Unit = {
    val maybeCallee = localLookup(callee)
    if (maybeCallee.isDefined)
      maybeCallee.get ! msg
    else
      localLookup(caller).
        foreach(_ ! Failure(callee, msg, "unknown callee actor: " + callee))
  }
}

class ActorDAgency extends Agency {
  def pend(caller: Card, callee: Card, msg: AnyRef): Unit = {
    val maybeCallee = localLookup(callee)
    if (maybeCallee.isDefined)
      maybeCallee.get ! msg
    else
      localLookup(caller).
        foreach(_ ! Failure(callee, msg, "unknown callee actor: " + callee))
  }
}

case class Node(host: String, port: Int)

trait Request

trait NodeManager {
  protected val workers = new mutable.HashMap[Node, NodeWorker]

  def worker(n: Node) = synchronized { 
      workers.getOrElse(n, {
        val w = createNodeWorker(n)
        workers += (n -> w)
        w
      })
  }

  def workerDone(n: Node) = synchronized { workers -= n }

  def createNodeWorker(n: Node): NodeWorker
}

abstract class NodeWorker(manager: NodeManager, node: Node) {
  def alive: Boolean

  protected val pendingRequests: BlockingQueue[PendingRequest] = createPendingRequestsQueue

  def createPendingRequestsQueue: BlockingQueue[PendingRequest] = new ArrayBlockingQueue[PendingRequest](60)

  def pend(request: Request): Unit = 
      pend(request, Actor.self)

  def pend(request: Request, requestor: Actor): Unit = 
    pendingRequests.put(new PendingRequest(request, List(requestor)))

  def call(request: Request): AnyRef = { // Synchronous version of pend.
    var result: AnyRef = null
    Actor.actor {                        // Use a throwaway actor to call pend and wait for result message.
      Actor.react {                      // TODO: Explore more efficient version with Actor.freshReplyChannel.
        case NodePend(r) => 
          pend(r, Actor.self)
          Actor.self.act
        case x: AnyRef => 
          result = x
          Actor.self.exit
      }
    } !? NodePend(request)
    result
  }

  def run {
    try {
      while (alive) {
        val pr = pendingRequests.take
        if (pr != null) {
        }
      }
    } finally {
      runDone
    }
  }

  def runDone: Unit =  {
    manager.workerDone(node)

    while (!pendingRequests.isEmpty) {
      val pr = pendingRequests.take
      if (pr != null)
          pr.callbacks.foreach(_ ! NodeError("runDone"))
    }
  }
}

case class NodePend(request: Request)
case class NodeError(message: String)

class PendingRequest(request: Request, protected var callbacks_i: List[Actor]) {
  def addCallback(a: Actor): Unit = synchronized {
    if (a != null)
        callbacks_i = a :: callbacks_i
  }
  def callbacks = callbacks_i
}

// ----------------------------------------------

class SNodeManager extends NodeManager {
  def createNodeWorker(n: Node): NodeWorker = {
    val w = new SNodeWorker(this, n)
    (new Thread(w)).start
    w
  }
}

class SNodeWorker(manager: NodeManager, node: Node) 
  extends NodeWorker(manager: NodeManager, node) with Runnable {
  val s = new Socket(node.host, node.port)
  def alive = s.isConnected && !s.isClosed
}

