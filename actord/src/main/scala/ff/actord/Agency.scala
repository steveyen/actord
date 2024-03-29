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

case class Card(base: String, more: String) {
  def ~> (msg: AnyRef): Unit = 
    Agency.default.pend(Actor.self, this, msg)

  def ~> (msg: AnyRef, timeout: Long, cont: PartialFunction[Any, Unit]): Unit = 
    Agency.default.pend(Actor.self, this, msg, timeout, cont)
}

object Agency {
  private var default_i: Agency = new LocalAgency

  def default_!(a: Agency) = synchronized { default_i = a}
  def default              = synchronized { default_i }

  def initDefault(defaultAgency: Agency) = default_!(defaultAgency)

  def myCard = Agency.default.localCardFor(Actor.self)

  // Special card used to reference the actor that creates more actors.
  //
  val factoryCard                     = Card("", "_factoryActor")
  def factoryCard(base: String): Card = Card(base, factoryCard.more)
}

trait Agency {
  def localActorFor(c: Card): Option[Actor]
  def localCardFor(someLocalActor: Actor): Card

  def pend(caller: Actor, callee: Card, msg: AnyRef): Unit
  def pend(caller: Card, callee: Card, msg: AnyRef): Unit

  def pend(callerActor: Actor, callee: Card, msg: AnyRef, timeout: Long, 
           cont: PartialFunction[Any, Unit]): Unit = {
    callerActor.asInstanceOf[AgencyActor].
                registerContinuation(localCardFor(callerActor),
                                     callee, msg, timeout: Long, cont)

    pend(callerActor, callee, msg)
  }
}

trait AgencyActor { self: Actor =>
  def registerContinuation(caller: Card, callee: Card, msg: AnyRef, timeout: Long, 
                           cont: PartialFunction[Any, Unit]): Unit = {
    val frame = Frame(caller, callee, msg)

    reactToAgencyContinuations += (frame -> cont)
  }

var lastFrame: Frame = null

  /**
   * Instead of using react(), an actor can use the reactToAgency() alternative 
   * which allows a more natural, apparently linear style of programming with 
   * nested case statements.
   */
  def reactToAgency(body: PartialFunction[Any, Unit]): Nothing = 
      react(reactToAgencyPF orElse body)

  protected val reactToAgencyContinuations = 
    new mutable.HashMap[Frame, PartialFunction[Any, Unit]] with
        mutable.SynchronizedMap[Frame, PartialFunction[Any, Unit]]

  val reactToAgencyPF = new PartialFunction[Any, Unit] {
    def isDefinedAt(x: Any): Boolean = 
      x match {
        case Reply(callee: Card, originalMsg: AnyRef, reply: AnyRef) =>
          reactToAgencyContinuations.contains(Frame(Agency.default.localCardFor(AgencyActor.this), callee, originalMsg))

        case Failure(callee: Card, originalMsg: AnyRef, failReason: AnyRef) =>
          reactToAgencyContinuations.contains(Frame(Agency.default.localCardFor(AgencyActor.this), callee, originalMsg))

        case _ =>
          false
      }

    def apply(x: Any): Unit = 
      x match {
        case Reply(callee: Card, originalMsg: AnyRef, reply: AnyRef) =>
          val f = Frame(Agency.default.localCardFor(AgencyActor.this), callee, originalMsg)
          val c = reactToAgencyContinuations.get(f)
          if (c.isDefined) {
            reactToAgencyContinuations -= f
            c.get.apply(OnReply(reply))
          }

        case Failure(callee: Card, originalMsg: AnyRef, failReason: AnyRef) =>
          val f = Frame(Agency.default.localCardFor(AgencyActor.this), callee, originalMsg)
          val c = reactToAgencyContinuations.get(f)
          if (c.isDefined) {
            reactToAgencyContinuations -= f
            c.get.apply(OnFailure(failReason))
          }

        case _ =>
      }
  }
}

case class Frame       (caller: Card, callee: Card, msg: AnyRef)
case class Reply       (callee: Card, originalMsg: AnyRef, reply: AnyRef)
case class Failure     (callee: Card, originalMsg: AnyRef, failReason: AnyRef)
case class CreateActor (callee: Card, msg: AnyRef, pool: ActorPool)

case class OnReply   (reply: AnyRef)
case class OnFailure (failReason: AnyRef)

// ----------------------------------------------

// Represents a fluid set of actors, which the system might
// optionally persist and/or flush under memory pressure.
//
trait ActorPool { 
  def offer(card: Card, actor: Actor, expiration: Long, async: Boolean): Boolean
  def offer(key: String, data: Array[Byte], expiration: Long, attachment: AnyRef, async: Boolean): Boolean
}

// ----------------------------------------------

class LocalAgency extends Actor with Agency {
  trapExit = true

  def act = {
    trapExit = true
    Actor.loop {
      Actor.react {
        // We link to all the local actors that ever invoked this 
        // agency in order to get their Exit notifications for cleanup.
        //
        case Exit(exitingLocalActor, reason) => synchronized {
          localCards.get(exitingLocalActor).foreach(localUnregister _)
          this.unlink(exitingLocalActor)
        }

        case ExitWatch(otherLocalActor) =>
          this.link(otherLocalActor)
          reply(true)

        case ExitUnwatch(otherLocalActor) =>
          this.unlink(otherLocalActor)
          reply(true)

        case _ => /* NO-OP */
      }
    }
  }

  case class ExitWatch(a: Actor)
  case class ExitUnwatch(a: Actor)

  protected var nextCard: Long = 0L
  protected val localActors = new mutable.HashMap[Card, Actor] 
  protected val localCards  = new mutable.HashMap[AbstractActor, Card] 

  def localRegister(c: Card, a: Actor): Unit = synchronized {
    localCards.get(a).foreach(localUnregister _)
    localCards  += (a -> c)
    localActors += (c -> a)
    this !? ExitWatch(a)
  }

  def localUnregister(c: Card): Unit = synchronized {
    localActors.get(c).foreach(a => {
      localCards -= a
      this !? ExitUnwatch(a)
    })
    localActors -= c
  }

  def localActorFor(c: Card): Option[Actor] = 
    synchronized { localActors.get(c) }

  def localCardFor(someLocalActor: Actor): Card = synchronized { 
    localCards.get(someLocalActor).
               getOrElse({
                 nextCard += 1
                 val c = Card(localBase, nextCard.toString)
                 localRegister(c, someLocalActor)
                 c
               })
  }

  def localBase = ""

  def pend(caller: Actor, callee: Card, msg: AnyRef): Unit = 
      pend(localCardFor(caller), callee, msg)

  def pend(caller: Card, callee: Card, msg: AnyRef): Unit = {
    val maybeCallee = localActorFor(callee)
    if (maybeCallee.isDefined)
      maybeCallee.get ! msg
    else
      failure(caller, callee, msg, "unknown local callee: " + callee)
  }

  def failure(caller: Card, callee: Card, msg: AnyRef, failReason: AnyRef): Unit = 
    localActorFor(caller).
      foreach(_ ! Failure(callee, msg, failReason))

  start
}

// ----------------------------------------------

class ActorDAgency(host: String, port: Int) extends LocalAgency {
  val directPrefix = "actord://" // For directly addressed Nodes.

  override val localBase = directPrefix + host + ":" + port

  val nodeManager: NodeManager = createNodeManager
  def createNodeManager = new SNodeManager

  val receptionist: AnyRef = startReceptionist
  def startReceptionist = new SReceptionist(host, port, this, nodeManager.serializer)

  // --------------------------------------

  override def pend(caller: Card, callee: Card, msg: AnyRef): Unit =
    if (callee.base == null ||
        callee.base.length <= 0 ||
        callee.base == localBase) {
      pendLocal(caller, callee, msg)
    } else
      pendRemote(caller, callee, msg)

  def pendLocal(caller: Card, callee: Card, msg: AnyRef): Unit = 
    super.pend(caller, callee, msg)

  def pendRemote(caller: Card, callee: Card, msg: AnyRef): Unit = 
    try {
      val nodeWorker = nodeManager.workerFor(nodeFor(callee))
      if (nodeWorker != null)
        localActorFor(caller).
          foreach(nodeWorker.pend(_, callee, msg))
      else
        failure(caller, callee, msg, "unknown callee node: " + callee)
    } catch {
      case ex => failure(caller, callee, msg, ex)
    }

  def nodeFor(c: Card): Node = {
    var n = nodeForDirect(c)
    if (n == null)
        n = nodeForIndirect(c)
    return n
  }

  def nodeForDirect(c: Card): Node = { // Returns a Node if it's an explicit, direct address.
    if (c.base.startsWith(directPrefix)) {
      val hostPort = c.base.substring(directPrefix.length).split(':')
      if (hostPort.length == 2)
        return Node(hostPort(0), Integer.parseInt(hostPort(1)))
    }
    null
  }

  def nodeForIndirect(c: Card): Node = {
    // TODO: Do a hash or crush into a hierarchy of servers.
    null
  }
}

