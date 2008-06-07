package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

object SampleUsingAgency {
  def main(args: Array[String]) {}

  val agency = new ActorDAgency("127.0.0.1", 11411)

  agency.localRegister(createActorCard, 
    actor { 
      loop { 
        react {
          case CreateActor(cardBase, msg, pool) => 
            cardBase.split("/")(0) match {
              case "chatRoom" =>
                val a = new ChatRoom(Card(cardBase, ""))
                pool.offer(a.myCard, a)
              case _ =>
            }
          case _ =>
        }
      }
    }
  )

  Agency.initDefault(agency)
}

class ChatRoom(val myCard: Card) extends Actor {
  var msgs: List[ChatRoomSay] = Nil
  def act {
    loop {
      react {
        case s: ChatRoomSay => 
          if (!msgs.exists(_ == s)) // Duplicate check for idempotency.
            msgs = s :: msgs
        case m @ ChatRoomView(viewer) =>
          viewer ~> Reply(myCard, m, msgs)
        case _ =>
      }
    }
  }
  start
}

case class ChatRoomSay(who: String, when: Long, text: String)
case class ChatRoomView(viewer: Card)

