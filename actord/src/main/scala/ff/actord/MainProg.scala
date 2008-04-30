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

import java.io._
import java.net._

import org.slf4j._

import ff.actord.Util._
import ff.collection._

abstract class MainProg {
  var storePath: String = null
  
  def default_port             = "11211"
  def default_limitMem         = "64"
  def default_storeInterval    = "5000"
  def default_storeLogFileSize = "10000000"

  /**
   * Start a server, parsing command-line arguments.
   */
  def start(args: Array[String]) {
    import MainFlag._
    
    val flagValues = parseFlags(args, flags)

    for (FlagValue(flag, values) <- flagValues) {
      if (flag == FLAG_ERR) {
        println("error: " + flag.specs.mkString(" | ") + " : " + values.mkString(" ").trim)
        System.exit(1)
      }

      if (flag.name == "help") {
        println("actord -- simple mesh of actors\n")
        println(" version : " + MServer.version)
        println(" usage   : <java-invocation> [flags*]\n")
        for (s <- flags) {
          println(s.specs.mkString(" | "))
          println(" " + s.description.split("\n").mkString("\n "))
        }
        System.exit(1)
      }
    }
    
    start(immutable.Map(flagValues.map(x => (x.flag.name -> x.value)):_*))
  }
  
  def start(args: immutable.Map[String, List[String]]) {
    def arg(flagName: String, defaultVal: String) =
      args.get(flagName).
           flatMap(_.headOption).
           getOrElse(defaultVal)
  
    val port      = arg("port",     default_port).toInt
    val limitMem  = arg("limitMem", default_limitMem).toLong * 1024L * 1024L
    val availCpus = Runtime.getRuntime.availableProcessors
    
    storePath = arg("storePath", null)
    
    val storeInterval    = arg("storeInterval",    default_storeInterval).toInt
    val storeLogFileSize = arg("storeLogFileSize", default_storeLogFileSize).toLong

    val server = createServer(availCpus, limitMem)

    startAcceptor(server, availCpus, port)
    startPersister(server, storeInterval, storeLogFileSize)

    println("limit memory      : " + limitMem)
    println("available cpus    : " + availCpus)
    println("listening on port : " + port)

    if (storePath != null) {
      println("storage path         : " + storePath)
      println("  check interval ms  : " + storeInterval)
      println("  log file max bytes : " + storeLogFileSize)
    }
  }
  
  // ------------------------------------------------------
  
  def startAcceptor(server: MServer, numProcessors: Int, port: Int): Unit
  
  def createProtocol: MProtocol = new MProtocol

  // ------------------------------------------------------
  
  def createServer(numProcessors: Int, limitMem: Long) = {
    if (storePath == null) {
      new MServer(numProcessors, limitMem) // Just an in-memory only server.
    } else {
      val store: MServerStorage = new MServerStorage(new File(storePath), numProcessors)
      new MServer(numProcessors, limitMem) {
        override def createSubServer(id: Int): MSubServer = 
          new MPersistentSubServer(id, limitMem / subServerNum, store.subStorages(id))
      }
    }
  }
  
  // ------------------------------------------------------

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
  def flags = List(
    Flag("limitMem", 
             "-m <num>" :: Nil,
             "Use <num> MB memory max for data; default is " + default_limitMem + "."),
    Flag("port", 
             "-p <num>" :: Nil,
             "Listen on port <num>; default is " + default_port + "."),
    Flag("storePath", 
             "-s_path <dir_path>" :: Nil,
             "Persist data to directory <dir_path>; default is no persistence."),
    Flag("storeInterval", 
             "-s_interval <millisecs>" :: Nil,
             "Check for dirty data that needs persistence; default is " + default_storeInterval + "."),
    Flag("storeLogFileSize", 
             "-s_log_file_size <bytes>" :: Nil,
             "Max size for an individual persistence log file; default is " + default_storeLogFileSize + "."),
    Flag("help", 
             "-h" :: "-?" :: "--help" :: Nil,
             "Show the version of the server and a summary of options."),
    Flag("verbose", 
             "-v" :: Nil,
             "Be verbose during processing; print out errors and warnings."),
    Flag("veryVerbose", 
             "-vv" :: Nil,
             "Be even more verbose; for example, also print client requests and responses.")
//  Flag("ipAddr", 
//           "-l <ip_addr>" :: Nil,
//           "Listen on <ip_addr>; default to INDRR_ANY.\n" +
//             "This is an important option to consider for security.\n" +
//             "Binding to an internal or firewalled network interface is suggested."),
//  Flag("noExpire", 
//           "-M" :: Nil,
//           "Instead of expiring items when max memory is reached, throw an error."),
//  Flag("maxConn", 
//           "-c <num>" :: Nil,
//           "Use <num> max simultaneous connections; the default is 1024."),
//  Flag("growCore", 
//           "-r" :: Nil,
//           "Raise the core file size limit to the maximum allowable."),
//  Flag("daemon", 
//           "-d" :: Nil,
//           "Run server as a daemon."),
//  Flag("username", 
//           "-u <username>" :: Nil,
//           "Assume the identity of <username> (only when run as root)."),
//  Flag("lockMem", 
//           "-k" :: Nil,
//           "Lock down all paged memory. This is a somewhat dangerous option with large caches, " +
//             "so consult the docs for configuration suggestions."),
//  Flag("licenses", 
//           "-i" :: Nil,
//           "Print server and component licenses."),
//  Flag("pidFile", 
//           "-P <filename>" :: Nil,
//           "Print pidfile to <filename>, only used under -d option.")
  )
}

// ------------------------------------------------------

class MainProgSimple extends MainProg {
  def startAcceptor(server: MServer, numProcessors: Int, port: Int): Unit = 
    (new SAcceptor(server, createProtocol, numProcessors, port)).start
}

// ------------------------------------------------------

class MainProgMina extends MainProg {
  import org.apache.mina.common._
  import org.apache.mina.filter.codec._
  import org.apache.mina.filter.codec.demux._
  import org.apache.mina.transport.socket.nio._

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
  def createHandler(server: MServer): IoHandler            = new MMinaHandler(server)
  def createAcceptor(numProcessors: Int): IoAcceptor       = new NioSocketAcceptor(numProcessors)
  def createCodecFactory: DemuxingProtocolCodecFactory     = new DemuxingProtocolCodecFactory
  def createCodecFilter(f: ProtocolCodecFactory): IoFilter = new ProtocolCodecFilter(f)
  
  def createMessageDecoder(server: MServer, protocol: MProtocol): MessageDecoder[MResponse] = new MMinaDecoder(server, protocol)
  def createMessageEncoder(server: MServer, protocol: MProtocol): MessageEncoder[MResponse] = new MMinaEncoder(server, protocol)
}

