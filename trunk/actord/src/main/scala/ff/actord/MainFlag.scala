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

/**
 * Parse command-line flags and arguments.
 */
object MainFlag
{
  def parseFlags(args: Array[String], flags: List[Flag],
                 title: String, version: String): (String, String) => String = {
    val flagValues = parseFlags(args, flags)

    for (FlagValue(flag, values) <- flagValues) {
      if (flag == FLAG_ERR) {
        println("error: " + flag.specs.mkString(" | ") + " : " + values.mkString(" ").trim)
        System.exit(1)
      }

      if (flag.name == "help") {
        println(title)
        println(" version : " + version)
        println(" usage   : <java-invocation> [flags*]\n")
        for (s <- flags) {
          println(s.specs.mkString(" | "))
          println(" " + s.description.split("\n").mkString("\n "))
        }
        System.exit(1)
      }
    }
    
    val argsMap = immutable.Map(flagValues.map(x => (x.flag.name -> x.value)):_*)

    ((flagName: String, defaultVal: String) => { // This parseFlags returns a closure, which
       argsMap.get(flagName).                    // retrieves a named flag value or a defaultVal.
               flatMap(_.headOption).
               getOrElse(defaultVal)
     })
  }

  /**
   * Parse the flags on a command-line.  The returned list
   * might have an entry of FlagValue(FLAG_ERR, ...) to signal 
   * a parsing error for a particular parameter.
   */
  def parseFlags(args: Array[String], flags: List[Flag]): List[FlagValue] = {
    val xs = (" " + args.mkString(" ")). // " -a 1 -b -c 2"
               split(" -")               // ["", "a 1", "b", "c 2"]
    if (xs.headOption.
           map(_.trim.length > 0).
           getOrElse(false))
      List(FlagValue(FLAG_ERR, xs.toList))
    else
      xs.drop(1).                        // ["a 1", "b", "c 2"]
         toList.
         map(arg => { 
           val argParts = ("-" + arg).split(" ").toList
           flags.find(_.aliases.contains(argParts(0))).
                 map(flag => if (flag.check(argParts))
                               FlagValue(flag, argParts.tail)
                             else
                               FlagValue(FLAG_ERR, argParts)).
                 getOrElse(FlagValue(FLAG_ERR, argParts))
         })
  }
  
  /**
   * A sentinel singleton that signals parseFlags errors.
   */  
  val FLAG_ERR = Flag("err", "incorrect flag or parameter" :: Nil, "")  
}

// ------------------------------------------------------
  
case class FlagValue(flag: Flag, value: List[String])

case class Flag(name: String, specs: List[String], description: String) {
  val aliases = specs.map(_.split(" ")(0))
  
  def check(argParts: List[String]) = 
    specs.filter(spec => { 
                   val specParts = spec.split(" ")
                   specParts(0) == argParts(0) && 
                   specParts.length == argParts.length
                 }).
          isEmpty == false
}

