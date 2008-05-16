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

object Router {
  def main(args: Array[String]): Unit = (new Router).start(args)
}

class Router {
  def default_port   = "12221"
  def default_target = "127.0.0.1:11211"

  def start(args: Array[String]): Unit =
    start(MainFlag.parseFlags(args, flags, 
                              "router -- routes memcached messages, part of actord project", 
                              MServer.version))
  
  def start(arg: (String, String) => String) {
    val port   = arg("port",   default_port).toInt
    val target = arg("target", default_target)

    val targetParts = target.split(":")
    if (targetParts.length != 2) {
      println("error in target server parameter (-target <host:port>)")
      System.exit(1)
    }

    startAcceptor(targetParts(0), targetParts(1).toInt, port)

    println("listening on port : " + port)
    println("routing to target : " + target)
  }
  
  def startAcceptor(targetHost: String, targetPort: Int, port: Int): Unit = 
    (new SAcceptor(createProtocol(targetHost, targetPort), 1, port)).start

  def createProtocol(targetHost: String, targetPort: Int): MProtocol = 
    new RouterProtocol(targetHost, targetPort)

  // ------------------------------------------------------

  def flags = List(
    Flag("target", 
             "-target <host:port>" :: Nil,
             "Route requests to target server <host:port>; default is " + default_target + "."),
    Flag("port", 
             "-p <num>" :: Nil,
             "Listen on port <num>; default is " + default_port + "."),
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
//  Flag("pidFile", 
//           "-P <filename>" :: Nil,
//           "Print pidfile to <filename>, only used under -d option.")
  )
}

class RouterProtocol(targetHost: String, targetPort: Int) extends MProtocolServer(null) {
  // unfinished: 
  //  also can do a tee here, replicator, two-level cache
  /*
    proxyServer        = ServerProxy(Client(targetHost, targetPort))
    loggingProxyServer = LoggingProxy(ServerProxy(Client(targetHost, targetPort)))
    replicatingProxy   = ServerProxy(Client(targetHost, targetPort),
                                     Client(targetHost, targetPort))

    the localCache is the hard one...
      need individual specs
        read-thru get spec
        write-thru set spec

    localCache = ReadThruProxy(Client(targetHost, targetPort)))
    localCache = WriteThruProxy(Client(targetHost, targetPort)))

    Option[localServer]
    List[remoteServer]
    rules on how to choose between and process commands

    if sync-write-thru and set
      set into remote

    if localServer
      try local first
      if miss and sync-read-thru
        get from remote and add to local
        return hit or miss

   router matrix          synch          or async
     write-thru-to-remote early or late  early or late
     read-thru-to-remote  early or late  early or late

   MServer is a server interface
   MMainServer implements MServer, is local in-mem cache/server
   CClient is connection to remote memcached
   MProxyServer implements MServer, wraps a single CClient, forwards msgs to remote server
   MRouterServer forwards to 1 or more MServer

  */
}
