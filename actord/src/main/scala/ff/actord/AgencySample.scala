package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

object ChatRoomServer {
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
                  case title: String if title.length > 0 => title
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

// -----------------------------------------------

object ChatClient {
  // Starts the local process listening on port 11422...
  //
  Agency.initDefault(new ActorDAgency("127.0.0.1", 11422))

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("usage: scala ff.actord.ChatRoomClient roomKey userId msg")
      return
    }

    val roomBase = "chatRoom/" + args(0)

    createActorCard(roomBase) ~> "" // Create chat room, if not already.

    val roomCard = Card(roomBase, "")

    val u = actor {
      loop {
        react {
          case text: String =>
            roomCard ~> ChatRoomMessage(args(1), System.currentTimeMillis, text)
            roomCard ~> ChatRoomView(myCard)

          case Reply(roomCard, ChatRoomView(_), msgs) => 
            println(msgs)
            exit
        }
      }
    }

    u ! args(2)
  }
}


