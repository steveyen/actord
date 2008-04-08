package ff.mud.comet

import scala.actors.Actor
import scala.actors.Actor._
import net.liftweb.util.Helpers._
import net.liftweb.util._
import scala.xml.{NodeSeq}
import scala.collection.immutable.TreeMap
import net.liftweb.textile.TextileParser
import scala.xml.Text
import java.util.Date

class ChatServer extends Actor {
  def act = loop(Nil, Nil)

  def loop(chat: List[ChatLine], sessions: List[Actor]) {
    react {
      case ChatServerMsg(user, msg) if msg.length > 0 =>
        val chatu = (ChatLine(user, toHtml(msg), timeNow) :: chat).take(500)
        val toDistribute = chatu.take(15)
        sessions.foreach (_ ! ChatServerUpdate(toDistribute))
        loop(chatu, sessions)
    
      case ChatServerAdd(me) =>
        me ! ChatServerUpdate(chat.take(15))
        loop(chat, me :: sessions)
    
      case ChatServerRemove(me) => loop(chat, sessions.remove(_ == me))
    
      case _ => loop(chat, sessions)
    }
  }

  def toHtml(msg: String): NodeSeq = 
    TextileParser.parse(msg, Empty).            // parse it
                  map(_.toHtml.toList match {   // convert to html and get the first child (to avoid things being wrapped in <p>)
                        case Nil => Nil 
                        case x :: xs => x.child
                      }). 
                  getOrElse(Text(msg)) // if it wasn't parsable, then just return a Text node of the message
}

object ChatServer {
  val server = {
    val ret = new ChatServer
    ret.start
    ret
  }
}

case class ChatLine(user: String, msg: NodeSeq, when: Date)
case class ChatServerMsg(user: String, msg: String)
case class ChatServerUpdate(msgs: List[ChatLine])
case class ChatServerAdd(me: Actor)
case class ChatServerRemove(me: Actor)

