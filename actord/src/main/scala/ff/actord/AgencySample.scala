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
          case CreateActor(callee, msg, server) => callee.base.split("/")(0) match {
            case "invoice" =>
              val c = Card(callee.base, "")
              val a = new InvoiceActor(c)
              // register(server, , new InvoiceActor(c)) // TODO!
            case _ =>
          }
          case _ =>
        }
      }
    }
  )

  Agency.init(agency)
}

class InvoiceActor(myCard: Card) extends Actor {
  def act {
    loop {
      react {
        case _ =>
      }
    }
  }
  start
}
