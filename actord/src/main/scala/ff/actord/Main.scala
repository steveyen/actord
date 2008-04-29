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
import java.io._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._
import ff.collection._

object Main
{
  def main_default(args: Array[String]) {
    new MainProg().start(args)
  }
  
  def main(args: Array[String]) {
    (new MainProg() {
      override def startAcceptor(server: MServer, numProcessors: Int, port: Int): Unit = 
        (new SAcceptor(server, createProtocol, numProcessors, port)).start
    }).start(args)
  }
  
  def main_with_example_of_custom_processing(args: Array[String]) {    
    // Two ways of customizing the server are by subclassing MServer
    // and providing your own override methods.  Or, you can use
    // the partial functions hooks if you want the flexibility
    // of pattern matching.  An example of the later...
    //
    (new MainProg() {
      override def createServer(numProcessors: Int, limitMem: Long): MServer = {
        val server = super.createServer(numProcessors, limitMem)
        server.getPf = myCustomGetPf orElse server.defaultGetPf
        server.setPf = myCustomSetPf orElse server.defaultSetPf
        server
      }
      
      def myCustomGetPf: MServer.MGetPf = { 
        case keys: Seq[String] if (keys.length == 1 && 
                                   keys(0) == "hello") => { 
          keys => { 
            // ... return your results as Iterator[MEntry] ...
            // ... or you can just return Iterator.empty ...
            //
            List(MEntry("hello", 0L, 0L, 5, "world".getBytes, 0L)).elements
          }
        }
      }

      def myCustomSetPf: MServer.MSetPf = { 
        case ("set", el, async) if (el.key == "hello") => { 
          (el, async) => { 
            // ... do your own processing with the input el data ...
            //
            true
          }
        }
      }
    }).start(args)
  }
}

// ------------------------------------------------------

class MainProg {
  var storePath: String = null
  
  /**
   * Start a server, parsing command-line arguments.
   */
  def start(args: Array[String]) {
    import MainFlag._
    
    val flagValueList = parseFlags(args, flagSpecs)

    for (FlagValue(spec, values) <- flagValueList) {
      if (spec == FLAG_ERR) {
        println("error: " + spec.specs.mkString(" | ") + " : " + values.mkString(" ").trim)
        System.exit(1)
      }

      if (spec.name == "help") {
        println("actord -- simple mesh of actors\n")
        println(" version : " + MServer.version)
        println(" usage   : <java-invocation> [flags*]\n")
        for (s <- flagSpecs) {
          println(s.specs.mkString(" | "))
          println(" " + s.description.split("\n").mkString("\n "))
        }
        System.exit(1)
      }
    }
    
    start(immutable.Map(flagValueList.map(x => (x.spec.name -> x.value)):_*))
  }
  
  def start(args: immutable.Map[String, List[String]]) {
    def arg(flagName: String, defaultVal: String) =
      args.get(flagName).
           flatMap(_.headOption).
           getOrElse(defaultVal)
  
    val port      = arg("port",     "11211").toInt
    val limitMem  = arg("limitMem", "64"   ).toLong * 1024L * 1024L
    val availCpus = Runtime.getRuntime.availableProcessors
    
    storePath = arg("storePath", null)
    
    val server = createServer(availCpus, limitMem)

    startAcceptor(server, availCpus, port)
    startPersister(server, 500, 1000000L)

    println("limit memory      : " + limitMem)
    println("available cpus    : " + availCpus)
    println("listening on port : " + port)
  }
  
  // ------------------------------------------------------
  
  def startAcceptor(server: MServer, numProcessors: Int, port: Int): Unit = 
    initAcceptor(server, createAcceptor(numProcessors)).bind(new InetSocketAddress(port))
  
  def initAcceptor(server: MServer, acceptor: IoAcceptor): IoAcceptor = {
    val codecFactory = createCodecFactory
    val protocol     = createProtocol

    codecFactory.addMessageDecoder(createMessageDecoder(server, protocol))
    codecFactory.addMessageEncoder[MResponse](classOf[MResponse], 
                                              createMessageEncoder(server, protocol))
    
    acceptor.getFilterChain.
             addLast("codec", createCodecFilter(codecFactory))  
    acceptor.setHandler(createHandler(server))
    acceptor
  }
  
  // Here are simple constructors that can be easily overridden by subclasses.
  //
  def createProtocol: MProtocol                             = new MProtocol
  def createHandler(server: MServer): IoHandler             = new MMinaHandler(server)
  def createAcceptor(numProcessors: Int): IoAcceptor        = new NioSocketAcceptor(numProcessors)
  def createCodecFactory: DemuxingProtocolCodecFactory      = new DemuxingProtocolCodecFactory
  def createCodecFilter(f: ProtocolCodecFactory): IoFilter  = new ProtocolCodecFilter(f)
  
  def createMessageDecoder(server: MServer, protocol: MProtocol): MessageDecoder[MResponse] = new MMinaDecoder(server, protocol)
  def createMessageEncoder(server: MServer, protocol: MProtocol): MessageEncoder[MResponse] = new MMinaEncoder(server, protocol)

  // ------------------------------------------------------
  
  def createServer(numProcessors: Int, limitMem: Long) = {
    val store: MServerStorage = 
      if (storePath != null)
        new MServerStorage(new File(storePath), numProcessors)
      else
        null

    new MServer(numProcessors, limitMem) {
      override def createSubServer(id: Int): MSubServer = 
        if (store != null)
          new MPersistentSubServer(id, limitMem / subServerNum, store.subStorages(id))
        else
          super.createSubServer(id)
    }
  }
  
  def createPersister(subServers: Seq[MSubServer], checkInterval: Int, limitFileSize: Long) =
    new MPersister(subServers, checkInterval, limitFileSize)
    
  def startPersister(server: MServer, checkInterval: Int, limitFileSize: Long): Unit =
    if (storePath != null)
      new Thread(createPersister(server.subServerList, checkInterval, limitFileSize)).start
  
  // ------------------------------------------------------

  /**
   * Specifications of command-line parameters or flags.  
   * Subclasses might override this method to add/remove entries to the list.
   */
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
    FlagSpec("storePath", 
             "-s <dir_path>" :: Nil,
             "Persist data to directory <dir_path>; the default is ./data directory."),
    FlagSpec("help", 
             "-h" :: "-?" :: "--help" :: Nil,
             "Show the version of the server and a summary of options."),
    FlagSpec("verbose", 
             "-v" :: Nil,
             "Be verbose during processing; print out errors and warnings."),
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
}
