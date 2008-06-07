package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

object ChatRoomSampleUsingAgency {
  def main(args: Array[String]) {}

  // Starts the local process listening on port 11411...
  //
  val agency = new ActorDAgency("127.0.0.1", 11411)

  // Register a local actor that can create other actors.
  // Remote clients can ask this server to create some
  // chatRoom's by doing...
  //
  //   createActorCard("chatRoom/club23") ~> "Welcome to Club 23!"
  //   createActorCard("chatRoom/hallway111") ~> "Hello Hallway 111"
  //
  agency.localRegister(createActorCard, 
    actor { 
      loop { 
        react {
          case CreateActor(cardBase, msg, pool) => 
            val splitArr = cardBase.split("/")

            splitArr(0) match {
              case "chatRoom" =>
                val a = new ChatRoom(Card(cardBase, ""), msg match {
                  case title: String => title
                  case _ => splitArr(1)
                })
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

class ChatRoom(val myCard: Card, roomTitle: String) extends Actor {
  var msgs: List[ChatRoomMessage] = Nil
  def act {
    loop {
      react {
        case s: ChatRoomMessage => 
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

case class ChatRoomMessage(who: String, when: Long, text: String)
case class ChatRoomView(viewer: Card)

class ChatUser(name: String) {
  def actor {
    var currRoomName: String = "foyer"
    loop {
      react {
        case ChatUserSay(what) =>
          val roomCard = Card("chatRoom/" + currRoomName, "")
          roomCard ~> ChatRoomMessage(name, System.currentTimeMillis, what)
          roomCard ~> ChatRoomView(myCard)

        case Reply(roomCard, ChatRoomView(_), msgs) => 
          println(msgs)
      }
    }
  }
}

case class ChatUserSay(what: String)

