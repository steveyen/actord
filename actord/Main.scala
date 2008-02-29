package ff.actord

import java.net._
import java.nio.charset._
import java.util.concurrent._

import org.slf4j._

import org.apache.mina.common._
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._
import org.apache.mina.transport.socket.nio._

import ff.actord.Util._

object Main
{
  def main(args: Array[String]) {
    startAcceptor(Runtime.getRuntime.availableProcessors, 11211)
    println("listening on port " + 11211)
  }
  
  def startAcceptor(numProcessors: Int, port: Int) = 
    initAcceptor(new NioSocketAcceptor(numProcessors)).bind(new InetSocketAddress(port))
  
  def initAcceptor(acceptor: IoAcceptor) = {
    val codecFactory = new DemuxingProtocolCodecFactory

    codecFactory.addMessageDecoder(new MDecoder)
    codecFactory.addMessageEncoder(classOf[List[MResponse]], new MEncoder)
    
    acceptor.getFilterChain.
             addLast("codec", new ProtocolCodecFilter(codecFactory))  
    acceptor.setHandler(new MServer)
    acceptor
  }
}
