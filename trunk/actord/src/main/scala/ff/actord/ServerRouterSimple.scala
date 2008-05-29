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

import ff.actord.Util._
import ff.actord.MProtocol._

class SServerRouter(host: String, port: Int) extends MServerRouter { // A simple router to a single target server.
  val target = new SRouterTarget(host, port)                   

  def chooseTarget(spec: MSpec, clientSession: MSession, 
                   cmdArr: Array[Byte], cmdArrLen: Int, cmdLen: Int): MRouterTarget = target
}

class SRouterTarget(host: String, port: Int) extends MRouterTarget { // A simple router target.
  def readResponse(buf: Array[Byte], offset: Int, length: Int): Int =
    is.read(buf, offset, length)

  protected var s  = new Socket(host, port) // The downstream target server to route to.
  protected var is = s.getInputStream
  protected var os = s.getOutputStream
  protected var bs = new BufferedOutputStream(os)

  def write(m: String): Unit                                = bs.write(stringToArray(m))
  def write(a: Array[Byte]): Unit                           = bs.write(a, 0, a.length)
  def write(a: Array[Byte], offset: Int, length: Int): Unit = bs.write(a, offset, length)

  def writeFunc = (a: Array[Byte], offset: Int, length: Int) => bs.write(a, offset, length)

  def isClosed = s == null

  def close: Unit = {
    if (s != null)
        s.close
    s  = null
    is = null
    os = null
    bs = null
  }

  def writeFlush: Unit = 
    try {
      bs.flush
    } catch {
      case _ => close
    }
}

// --------------------------------------------------

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

   router matrix          synch            or async
     write-thru-to-remote before or after  before or after
     read-thru-to-remote  before or after  before or after

   syncLevel is 0-sync, 1-sync, Quorum-sync, N-sync, All-sync

   // By unified, we mean this pseudocode handles read and write ops.
   //
   // The primary might be the local store.
   // The replicas might be 1 or more remote stores.
   //
   // Analytics and remote logging might be handled as just yet another replica?
   //
   def unifiedOp(...) = { 
     var repNBefore = _
     var repNAfter  = _

     if (startReplicas == before)
       repNBefore = startReplicas(syncLevel)

     var pri = doPrimary

     if (startReplicas == after &&
         pri & repNBefore are not satisfied)
       repNAfter = startReplicas(syncLevel)

     wait until (pri & repNBefore & repNAfter are satisfied) or timeout
   }

   MServer is a server interface
   MMainServer implements MServer, is local in-mem cache/server
   MServerProxy implements MServer, forwards msgs to a single remote server at host:port
   MServerRouter forwards to 1 or more downstream target servers
  */

