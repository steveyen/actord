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

object Main
{
  def main(args: Array[String]) {
    // Implementations include simple blocking i/o, or grizzly/NIO, or mina/NIO...
    //
    // new MainProgSimple().start(args)
    // new MainProgGrizzly().start(args)
    // new MainProgMina().start(args)
    
    new MainProgMina().start(args)
  }
  
  def main_with_example_of_custom_processing(args: Array[String]) {    
    // Two ways of customizing the server are by subclassing MServer
    // and providing your own override methods.  Or, you can use
    // the partial functions hooks if you want the flexibility
    // of pattern matching.  An example of the later...
    //
    (new MainProgMina() {
      override def createServer(numProcessors: Int, limitMem: Long): MServer = {
        val server = super.createServer(numProcessors, limitMem).asInstanceOf[MMainServer]
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

