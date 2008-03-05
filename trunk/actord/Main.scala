/**
 * Copyright 2008 Steve Yen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val flagValueList = parseFlags(args)

    for (FlagValue(spec, values) <- flagValueList) {
      if (spec == errSpec) {
        println("error: " + spec.specs.mkString(" | ") + " : " + values.mkString(" ").trim)
        System.exit(1)
      }

      if (spec.name == "help") {
        println("actord -- simple mesh of actors\n")
        println(" version : TBD")
        println(" usage   : <java-invocation> [flags*]\n")
        for (s <- flagSpecs) {
          println(s.specs.mkString(" | "))
          println(" " + s.description.split("\n").mkString("\n "))
        }
        System.exit(1)
      }
    }
    
    val m = immutable.Map(flagValueList.map(x => (x.spec.name -> x)):_*)
    
    val port      = getFlagValue(m, "port",     "11211").toInt
    val limitMem  = getFlagValue(m, "limitMem", "64"   ).toInt * 1024 * 1024
    val availCpus = Runtime.getRuntime.availableProcessors
    
    startAcceptor(new MServer(availCpus, limitMem), availCpus, port)

    println("limit memory      : " + limitMem)
    println("available cpus    : " + availCpus)
    println("listening on port : " + port)
  }
  
  // ------------------------------------------------------
  
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

  def flagSpecs = List(
//  FlagSpec("ipAddr", 
//           "-l <ip_addr>" :: Nil,
//           "Listen on <ip_addr>; default to INDRR_ANY.\nThis is an important option to consider for security.\nBinding to an internal or firewalled network interface is suggested."),
    FlagSpec("limitMem", 
             "-m <num>" :: Nil,
             "Use <num> MB memory max for object storage; the default is 64 MB."),
    FlagSpec("port", 
             "-p <num>" :: Nil,
             "Listen on port <num>, the default is port 11211."),
    FlagSpec("help", 
             "-h" :: "-?" :: "--help" :: Nil,
             "Show the version of the server and a summary of options."),
    FlagSpec("verbose", 
             "-v" :: Nil,
             "Be verbose during the event loop; print out errors and warnings."),
    FlagSpec("veryVerbose", 
             "-vv" :: Nil,
             "Be even more verbose; for example, also print client requests and responses.")
//  FlagSpec("noExpire", 
//           "-M" :: Nil,
//           "Instead of expiring items when max memory is reached, throw an error."),
//  FlagSpec("maxConn", 
//           "-c <num>" :: Nil,
//           "Use <num> max simultaneous connections; the default is 1024."),
//  FlagSpec("growCore", 
//           "-r" :: Nil,
//           "Raise the core file size limit to the maximum allowable."),
//  FlagSpec("daemon", 
//           "-d" :: Nil,
//           "Run server as a daemon."),
//  FlagSpec("username", 
//           "-u <username>" :: Nil,
//           "Assume the identity of <username> (only when run as root)."),
//  FlagSpec("lockMem", 
//           "-k" :: Nil,
//           "Lock down all paged memory. This is a somewhat dangerous option with large caches, so consult the docs for configuration suggestions."),
//  FlagSpec("licenses", 
//           "-i" :: Nil,
//           "Print server and component licenses."),
//  FlagSpec("pidFile", 
//           "-P <filename>" :: Nil,
//           "Print pidfile to <filename>, only used under -d option.")
  )
  
  // ------------------------------------------------------
  
  def parseFlags(args: Array[String]): List[FlagValue] = {
    val xs = (" " + args.mkString(" ")). // " -a 1 -b -c 2"
               split(" -")               // ["", "a 1", "b", "c 2"]
    if (xs.headOption.
           map(_.trim.length > 0).
           getOrElse(false))
      List(FlagValue(errSpec, xs.toList))
    else
      xs.drop(1).                        // ["a 1", "b", "c 2"]
         toList.
         map(arg => { 
           val argParts = ("-" + arg).split(" ").toList
           flagSpecs.find(_.flags.contains(argParts(0))).
                     map(spec => if (spec.check(argParts))
                                   FlagValue(spec, argParts.tail)
                                 else
                                   FlagValue(errSpec, argParts)).
                     getOrElse(FlagValue(errSpec, argParts))
         })
  }
  
  def getFlagValue(flagValues: immutable.Map[String, FlagValue],
                   flagName: String, defaultVal: String) =
    flagValues.get(flagName).map(_.value.head).getOrElse(defaultVal)
  
  case class FlagValue(spec: FlagSpec, value: List[String])

  case class FlagSpec(name: String, specs: List[String], description: String) {
    val flags = specs.map(_.split(" ")(0))
    
    def check(argParts: List[String]) = 
      specs.filter(
        spec => { 
          val specParts = spec.split(" ")
          specParts(0) == argParts(0) && 
          specParts.length == argParts.length
        }
      ).isEmpty == false
  }
  
  val errSpec = FlagSpec("err", "incorrect flag or parameter" :: Nil, "")
}
