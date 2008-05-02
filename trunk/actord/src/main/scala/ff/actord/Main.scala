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
    
    new MainProgSimple().start(args)
  }
}

