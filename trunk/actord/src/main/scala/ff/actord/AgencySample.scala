package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

// Example of an ActorD Agency based chat room server and client.
//
// Start a chat server...
//   scala -cp target/classes ff.actord.ChatRoomServer
//
object ChatRoomServer {
  def main(args: Array[String]) {}

  // Starts this server process listening on port 11411...
  //
  val agency = new ActorDAgency("127.0.0.1", 11411)

  // Register a local actor that can create other actors.
  // Remote clients can ask this server to create some
  // chatRoom's by doing...
  //
  agency.localRegister(createActorCard, 
    actor { 
      loop { 
        react {
          case CreateActor(cardBase, args, pool) => 
            println("CreateActor " + cardBase)
            val splitArr = cardBase.split("/")

            splitArr(0) match {
              case "chatRoom" =>
                args match {
                  case AddChatRoom(roomTitle, caller) =>
                    val r = new ChatRoom(Card(cardBase, ""), roomTitle)
                    val o = pool.offer(r.myCard, r, false)
                    if (o == true)
                        r.start
                    if (caller != null)
                        caller ~> Reply(myCard, args, r.myCard)
                    println("AddChatRoom: " + cardBase + " for caller: " + caller + " offer: " + o)
                  case _ =>
                }
              case _ =>
            }
          case _ =>
        }
      }
    }
  )

  Agency.initDefault(agency)
}

case class AddChatRoom(roomTitle: String, caller: Card)

class ChatRoom(val myCard: Card, roomTitle: String) extends Actor {
  var msgs: List[ChatRoomMessage] = Nil
  def act {
    loop {
      react {
        case s: ChatRoomMessage => 
          println("got msg " + s)
          if (!msgs.exists(_ == s)) // Duplicate check for idempotency.
            msgs = s :: msgs
        case m @ ChatRoomView(viewer) =>
          println("viewing by " + viewer)
          viewer ~> Reply(myCard, m, msgs)
        case _ =>
      }
    }
  }
}

case class ChatRoomMessage(who: String, when: Long, text: String)
case class ChatRoomView(viewer: Card)

// -----------------------------------------------

// Send a chat client message...
//   scala -cp target/classes ff.actord.ChatRoomClient [roomKey] [userId] [some-single-word-msg]
//
object ChatClient {
  // Starts the local process listening on port 11422...
  //
  Agency.initDefault(new ActorDAgency("127.0.0.1", 11422) {
    override def nodeForIndirect(c: Card): Node = {
      // Hardcode our expected one and only server Node in this example.
      // Normally, we'd instead do some consistent-hashing or CRUSH here.
      //
      return Node("127.0.0.1", 11411)
    }
  })

  case class ChatClientGo

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("usage: scala ff.actord.ChatRoomClient roomKey userId msg")
      return
    }

    val roomBase = "chatRoom/" + args(0)
    var roomCard = Card(roomBase, "")

    val u = actor {
      loop {
        react {
          case ChatClientGo =>
            // Create chat room, if not already...
            //
            createActorCard(roomBase) ~> AddChatRoom("room " + args(0) + " is fun!", myCard)

          case Reply(_, AddChatRoom(_, _), newRoomCard: Card) => 
            roomCard = newRoomCard
            self ! args(2)

          case text: String =>
            println("sending... text: " + text)
            roomCard ~> ChatRoomMessage(args(1), System.currentTimeMillis, text)
            roomCard ~> ChatRoomView(myCard)
            println("sending... done")

          case Reply(roomCard, ChatRoomView(_), msgs) => 
            println("msgs: " + msgs)
            System.exit(0)
        }
      }
    }

    u ! ChatClientGo

    println("running...")
  }
}


