package ff.actord

import scala.actors._
import scala.actors.Actor._

import ff.actord._
import ff.actord.Agency._

object SampleUsingAgency {
  def main(args: Array[String]) {
  }

  val agency = new ActorDAgency("127.0.0.1", 11411)

  agency.localRegister(createActorCard, 
    actor { loop { react {
      case CreateActor(callee, msg, server) =>
    } } }
  )

  Agency.init(agency)
}