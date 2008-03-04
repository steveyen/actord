package ff.actord

import scala.collection._

import java.net._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

object Main
{
  def main(args: Array[String]) {
    val flags = parseArgs(args)
    for ((spec, values) <- flags) {
      if (spec == errSpec) {
        println("error: " + spec.spec + " : " + values.mkString(" ").trim)
        System.exit(1)
      }
      if (spec.flag == "h") {
        println("actord -- simple mesh of actors\n")
        println(" version : TBD")
        println(" usage   : <java-invocation> [flags*]\n")
        for (s <- flagSpecs)
          println(s.spec + "\n " + s.description.split("\n").mkString("\n "))
        System.exit(1)
      }
    }

    startAcceptor(new MServer, 
                  Runtime.getRuntime.availableProcessors, 
                  11211)

    println("listening on port " + 11211)
  }
  
  def startAcceptor(server: MServer, numProcessors: Int, port: Int) = 
    initAcceptor(server, 
                 new NioSocketAcceptor(numProcessors)).bind(new InetSocketAddress(port))
  
  def initAcceptor(server: MServer, acceptor: IoAcceptor) = {
    val codecFactory = new DemuxingProtocolCodecFactory

    codecFactory.addMessageDecoder(new MDecoder)
    codecFactory.addMessageEncoder(classOf[List[MResponse]], new MEncoder)
    
    acceptor.getFilterChain.
             addLast("codec", new ProtocolCodecFilter(codecFactory))  
    acceptor.setHandler(new MHandler(server))
    acceptor
  }
  
  // ------------------------------------------------------
  
  def parseArgs(args: Array[String]): List[Pair[FlagSpec, Array[String]]] =
    if (args.isEmpty)
      Nil
    else {
      val xs = (" " + args.mkString(" ")). // " -a 1 -b -c 2"
                 split(" -")               // ["", "a 1", "b", "c 2"]
      if (xs.headOption.
             map(_.length > 0).
             getOrElse(false))
        List(Pair(errSpec, xs))
      else
        xs.drop(1).                        // ["a 1", "b", "c 2"]
           toList.
           map(arg => { 
             val argParts = arg.split(" ")
             flagSpecs.find(_.flag == argParts(0)).
                       map(spec => if (spec.check(argParts))
                                     Pair(spec, argParts)
                                   else
                                     Pair(errSpec, argParts)).
                       getOrElse(Pair(errSpec, argParts))
           })
    }

  case class FlagSpec(spec: String, description: String) {
    val parts = spec.split(" ").toList
    val flag  = parts(0).substring(1)
    
    def check(argParts: Array[String]) = parts.length == argParts.length
  }
  
  val errSpec = FlagSpec("incorrect flag or parameter", "")
  
  def flagSpecs = List(
//  FlagSpec("-l <ip_addr>",
//      "Listen on <ip_addr>; default to INDRR_ANY.\nThis is an important option to consider for security.\nBinding to an internal or firewalled network interface is suggested."),
//  FlagSpec("-d",
//      "Run server as a daemon."),
//  FlagSpec("-u <username>",
//      "Assume the identity of <username> (only when run as root)."),
    FlagSpec("-m <num>",
        "Use <num> MB memory max for object storage; the default is 64 MB."),
    FlagSpec("-M",
        "Instead of expiring items when max memory is reached, throw an error."),
    FlagSpec("-c <num>",
        "Use <num> max simultaneous connections; the default is 1024."),
//  FlagSpec("-k",
//      "Lock down all paged memory. This is a somewhat dangerous option with large caches, so consult the docs for configuration suggestions."),
    FlagSpec("-p <num>",
        "Listen on port <num>, the default is port 11211."),
    FlagSpec("-r",
        "Raise the core file size limit to the maximum allowable."),
    FlagSpec("-h",
        "Show the version of the server and a summary of options."),
    FlagSpec("-v",
        "Be verbose during the event loop; print out errors and warnings."),
    FlagSpec("-vv",
        "Be even more verbose; for example, also print client requests and responses.")
//  FlagSpec("-i",
//      "Print server and component licenses."),
//  FlagSpec("-P <filename>",
//      "Print pidfile to <filename>, only used under -d option.")
  )
}
