package bootstrap.liftweb

import net.liftweb.util._
import net.liftweb.http._
import net.liftweb.sitemap._
import net.liftweb.sitemap.Loc._
import Helpers._
import net.liftweb.mapper.{DB, ConnectionManager, Schemifier, DefaultConnectionIdentifier, ConnectionIdentifier}
import java.sql.{Connection, DriverManager}
import ff.mud.model._

/**
  * A class that's instantiated early and run.  It allows the application
  * to modify lift's environment
  */
class Boot {
  def boot {
    if (!DB.jndiJdbcConnAvailable_?) DB.defineConnectionManager(DefaultConnectionIdentifier, DBVendor)

    // where to search snippet
    LiftRules.addToPackages("ff.mud")     

    Schemifier.schemify(true, Log.infoF _, User)

    LiftRules.addTemplateBefore(User.templates)

    // Build SiteMap
    val entries = 
      Menu(Loc("Home", "/",     "Home")) :: 
      Menu(Loc("Chat", "/chat", "Chat")) :: 
      User.sitemap
    LiftRules.setSiteMap(SiteMap(entries:_*))

    S.addAround(User.requestLoans)

    // Exercise actord    
    new ff.actord.MainProg().start(new Array[String](0))
  }
}


object DBVendor extends ConnectionManager {
  def newConnection(name: ConnectionIdentifier): Can[Connection] = {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
      val dm = DriverManager.getConnection("jdbc:derby:data/db_derby;create=true")
      Full(dm)
    } catch {
      case e : Exception => e.printStackTrace; Empty
    }
  }
  def releaseConnection(conn: Connection) {conn.close}
}

