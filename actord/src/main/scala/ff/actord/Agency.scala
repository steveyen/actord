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
  def ~> (msg: AnyRef): Unit = Agency.default.pend(Actor.self, this, msg)
}

object Agency {
  private var default_i: Agency = new LocalAgency

  def default_!(a: Agency) = synchronized { default_i = a }
  def default              = synchronized { default_i }

  def myCard = Agency.default.localCardFor(Actor.self)

  val createActorCard = Card("", "_createActor")
}

trait Agency {
  def pend(caller: Actor, callee: Card, msg: AnyRef): Unit
  def pend(caller: Card, callee: Card, msg: AnyRef): Unit

  def localCardFor(someLocalActor: Actor): Card
}

case class Frame       (caller: Card, callee: Card, msg: AnyRef)
case class Reply       (callee: Card, originalMsg: AnyRef, reply: AnyRef)
case class Failure     (callee: Card, originalMsg: AnyRef, failReason: AnyRef)
case class CreateActor (callee: Card, msg: AnyRef, server: AnyRef)

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
        }
        case _ => /* NO-OP */
      }
    }
  }

  protected var nextCard: Long = 0L
  protected val localActors = new mutable.HashMap[Card, Actor] 
  protected val localCards  = new mutable.HashMap[Actor, Card] 

  def localRegister(c: Card, a: Actor): Unit = synchronized {
    localCards.get(a).foreach(localUnregister _)
    localCards  += (a -> c)
    localActors += (c -> a)
    this.link(a) // Do a link so we'll hear about the Exit of the actor.
  }

  def localUnregister(c: Card): Unit = synchronized {
    localActors.get(c).foreach(a => {
      localCards -= a
      this.unlink(a)
    })
    localActors -= c
  }

  def localActorFor(c: Card): Option[Actor] = synchronized { localActors.get(c) }

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
  val actordPrefix = "actord://"

  override val localBase = actordPrefix + host + ":" + port

  val receptionist = startReceptionist

  def startReceptionist: AnyRef = {
    // Start listening on the given port for memcached-speaking clients,
    // but also understand the Agency-related memcached-protocol semantics.
    //
    val receptionist = new MainProgSimple() {
      override def createServer(numProcessors: Int, limitMem: Long): MServer = {
        new MSubServer(0, limitMem) {
          override def set(el: MEntry, async: Boolean): Boolean = {
            // We sometimes dispatch the incoming set data to an actor.
            //
            if (el.key.startsWith("ad|")) {
              val keyParts = el.key.split("?")
              if (keyParts.length == 2) {
                val msg    = nodeManager.serializer.deserialize(el.data, 0, el.data.length)
                val callee = Card(keyParts(0), keyParts(1))
                val localA = localActorFor(callee)
                if (localA.isDefined) {
                    localA.get ! msg
                    return true
                } else if (callee.more == Agency.createActorCard.more) {
                    val a = localActorFor(Agency.createActorCard)
                    if (a.isDefined) {
                        a.get ! CreateActor(callee, msg, this)
                    } else {
                        return false // No actor creator was registered.
                    }
                } else {
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
            }

            super.set(el, async)
          }
        }
      }
    }

    receptionist.start((arg: String, defaultValue: String) => {
      arg match {
        case "portTCP" => port.toString
        case _         => defaultValue
      }
    })

    receptionist
  }

  // --------------------------------------

  val nodeManager: NodeManager = createNodeManager

  def createNodeManager = new SNodeManager

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
    if (c.base.startsWith(actordPrefix)) {
      val hostPort = c.base.substring(actordPrefix.length).split(":")
      if (hostPort.length == 2)
        return Node(hostPort(0), Integer.parseInt(hostPort(1)))
    } else {
      // TODO: Do a hash or crush into a hierarchy of servers.
    }
    null
  }
}

