package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

// Example of an ActorD Agency based chat room server and client.
//
// Start a chat server...
//   scala -cp target/classes ff.actord.ChatRoomServer [port]
//
object ChatRoomServer {
  def main(args: Array[String]) {
    val port = Integer.parseInt(args(0))

    // Starts this server process listening on port...
    //
    val agency = new ActorDAgency("127.0.0.1", port)

    // Register an actor that can create other actors, due
    // to special, parametrized requests coming from remote clients.
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
                      val o = pool.offer(r.myCard, r, 0, false)
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
      })

    Agency.initDefault(agency)
  }
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
//   scala -cp target/classes ff.actord.ChatRoomClient [serverList] [roomKey] [userId] [msg]
//
// The [serverList] argment is a comma-separated list of server host:port info,
// such as "127.0.0.1:11511,127.0.0.1:11611"
//
object ChatClient {
  def main(args: Array[String]) {
    val (roomKey, roomBase, roomCard, userId, msg) = 
      init(args, 11422,
           "usage: scala ff.actord.ChatRoomClient serverList roomKey userId msg")

    var currRoomCard: Card = roomCard

    // Here we have a stateless programming style, where the 
    // react case statements are all flat or at the same level.
    //
    // So, more info needs to be passed around in request and 
    // reply messages, with the benefits and problems similar 
    // to classic message-event-driven programming.
    //
    // While messages are in-flight and being processed on a 
    // remote server, the stateless programming style 
    // allows the client to keep minimal state.
    //
    val u = actor {
      loop {
        react {
          case ChatClientGo =>
            // Create chat room, if not already...
            //
            createActorCard(roomBase) ~> AddChatRoom("room " + roomKey + " is fun!", myCard)

          case Reply(_, AddChatRoom(_, _), newRoomCard: Card) => 
            currRoomCard = newRoomCard
            self ! msg

          case Failure(_, AddChatRoom(_, _), failReason) =>
            println("failure: could not add chat room: " + failReason)
            System.exit(0)

          case text: String =>
            println("sending... text: " + text)
            currRoomCard ~> ChatRoomMessage(userId, System.currentTimeMillis, text)
            currRoomCard ~> ChatRoomView(myCard)
            println("sending... done")

          case Reply(fromRoomCard, ChatRoomView(_), msgs) => 
            println("msgs: " + msgs)
            System.exit(0)

          case Failure(fromRoomCard, ChatRoomView(_), failReason) => 
            println("failure: could not view chat room: " + fromRoomCard + " reason: " + failReason)
            System.exit(0)

          case Failure(fromRoomCard, ChatRoomMessage(_, _, _), failReason) => 
            println("failure: could post to chat room: " + fromRoomCard + " reason: " + failReason)
            System.exit(0)
        }
      }
    }

    u ! ChatClientGo

    println("running...")
  }

  def init(args: Array[String], myPort: Int, usage: String) = {
    if (args.length != 4) {
      println(usage)
      System.exit(0)
    }

    val nodes: Array[Node] = args(0).split(',').map(hostPort => {
      val parts = hostPort.split(':')
      Node(parts(0), Integer.parseInt(parts(1)))
    })
    if (nodes.length < 1) {
      println("need at least 1 host:port in the serverList")
      System.exit(0)
    }

    // Starts the local process listening on port [myPort]...
    //
    Agency.initDefault(new ActorDAgency("127.0.0.1", myPort) {
      override def nodeForIndirect(c: Card): Node = 
        // Simple hashing in this example.
        // Normally, we'd instead do some consistent-hashing or CRUSH here.
        //
        nodes(c.base.hashCode % nodes.length)
    })

    val roomKey  = args(1)
    val roomBase = "chatRoom/" + roomKey
    val roomCard = Card(roomBase, "")
    val userId   = args(2)
    val msg      = args(3)

    (roomKey, roomBase, roomCard, userId, msg)
  }
}

case class ChatClientGo

// -----------------------------------------------

object ChatClientV2 {
  def main(args: Array[String]) {
    val (roomKey, roomBase, roomCard, userId, msg) = 
      ChatClient.init(args, 11422,
                      "usage: scala ff.actord.ChatRoomClientV2 serverList roomKey userId msg")

    var currRoomCard: Card = roomCard

    // Here we have a statefull programming style, where
    // we have nested case statements (partial functions).
    // 
    // This allows for increased readability and apparent linearity of the code,
    // at the cost of more state tracking in the client (in the platform code).
    //
    // Underneath the hood, all the below case clause closures are hoisted
    // into the enclosing, top-most react loop, even though they look like
    // they're nested with apparent linearity.
    //
    // Note that the ~> invocations remain completely asynchronous.
    //
    val u = actor {
      loop {
        react {
          case ChatClientGo =>
            // Create chat room, if not already...
            //
            createActorCard(roomBase) ~> (
              AddChatRoom("room " + roomKey + " is fun!", myCard), {
                case OnFailure(failReason) =>
                  println("failure: could not add chat room: " + failReason)
                  System.exit(0)

                case OnReply(newRoomCard: Card) => 
                  currRoomCard = newRoomCard
                  self ! msg
              })

          case text: String =>
            currRoomCard ~> (
              ChatRoomMessage(userId, System.currentTimeMillis, text), {
                case OnFailure(failReason) => 
                  println("failure: could post to chat room: " + currRoomCard + " reason: " + failReason)
                  System.exit(0)
              })

            currRoomCard ~> (
              ChatRoomView(myCard), {
                case OnFailure(failReason) => 
                  println("failure: could not view chat room: " + currRoomCard + " reason: " + failReason)
                  System.exit(0)

                case OnReply(msgs) => 
                  println("msgs: " + msgs)
                  System.exit(0)
              })
        }
      }
    }

    u ! ChatClientGo

    println("running...")
  }
}

case class OnReply   (reply: AnyRef)
case class OnFailure (failReason: AnyRef)
