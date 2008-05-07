/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

import scala.collection._

trait Node {
  def name: String
  def weight: Float
  def kind: String // Ex: process, server, shelf, rack, cabinet, row, datacenter, region, universe.
  def info: String // For app-specific information associated with the node, like "host:port" info.
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node
}

case class LeafNode(name: String, weight: Float, kind: String, info: String) extends Node {
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = null
}

trait BucketNode extends Node {
  def children: Seq[Node]
  def bucketType: String

  // val sumOfWeights: Float = children.foldLeft(0.0)((accum, child) => accum + child.weight)

  def crushHash32_2(name: String, x: Int): Int = 0 // TODO
  def crushHash32_2(x: Int, name: String): Int = 0 // TODO
  def crushHash32_4(x: Int, c: Node, r: Int, name: String): Int = 0 // TODO
}

case class ListBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "list"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = {
    for (i <- 0 until children.length) {
      val c = children(i)
      var w = crushHash32_4(x, c, r, name)
      w = w & 0xffff
      // w = w * sumOfWeights
      w = w >> 16
      if (w < c.weight)
        return c
    }
    null
  }
}

case class TreeBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "tree"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = null // TODO
}

case class UniformBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "uniform"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = {
    val o = crushHash32_2(x, name) & 0xffff
    val p = primes(crushHash32_2(name, x).toInt % children.length)
    val s = (x + o + (r + 1) * p) % children.length
    children(s)
  }
  val primes: Seq[Int] = Nil // TODO
}

case class StrawBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "straw"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = null // TODO
}

// -----------------------------------------------------------

class NodeMap(tree: NodeTree, steps: Seq[Step]) {
  def findReplicas(x: Int, nodeStatusMap: Map[String, Float]): Seq[Node] = 
    tree.doSteps(x, steps, nodeStatusMap)
}

class NodeTree(root: Node) { 
  def doSteps(x: Int, steps: Seq[Step], nodeStatusMap: Map[String, Float]): Seq[Node] = {
    val empty: Seq[Node] = Nil
    steps.foldLeft(empty)((accum, step) => step.doStep(x, this, accum, nodeStatusMap))
  }

  def visitNodes(p: (Node) => Unit): Unit = visitNodes(p, root)
  def visitNodes(p: (Node) => Unit, curr: Node): Unit =
    if (curr != null) {
      p(curr)
      curr match {
        case b: BucketNode => b.children.foreach(visitNodes(p, _))
	case _ => 
      }
    }

  val mapNodes = new mutable.HashMap[String, Node]

  visitNodes((x: Node) => mapNodes += (x.name -> x))

  def findNode(s: String): Option[Node] = mapNodes.get(s)

  def MAX_TOTAL_RETRY = 10
  def MAX_LOCAL_RETRY = 3

  /**
   * Choose numReplicas distinct Nodes of a certain kind.
   */
  def choose(x: Int, numReplicas: Int, kind: String, 
             startRep: Int,
             startNode: Node,
             nodeStatusMap: Map[String, Float]): Seq[Node] = 
      choose(x, numReplicas, kind, startRep, startNode, nodeStatusMap, MAX_TOTAL_RETRY, MAX_LOCAL_RETRY)

  def choose(x: Int, numReplicas: Int, kind: String, 
             startRep: Int,
             startNode: Node,
             nodeStatusMap: Map[String, Float],
             fTotalMax: Int, fLocalMax: Int): Seq[Node] = {
    var fTotal = 0
    var fLocal = 0
    var cur    = startNode
    var rep    = startRep
    var out: List[Node] = Nil

    def loopAgain {
      if (fTotal < fTotalMax) {
        fTotal += 1
        fLocal = 0
        cur = startNode // Try again with another path from the startNode.
      } else {
        fTotal = 0
        fLocal = 0
        cur = startNode 
        rep += 1        // Give up, returning less replicas than requested.
      }
    }

    while (rep < numReplicas) {
      val o = cur.chooseChild(x, rep, fTotal, fLocal, numReplicas)
      if (o != null) {
        if (o.kind == kind) {
          val outCollision = out.exists(_.name == o.name) 
          if (outCollision == false && 
              nodeOk(o, nodeStatusMap, x)) {
            fTotal = 0
            fLocal = 0
            cur = startNode
            rep += 1
            out = o :: out // Successfully found a new distinct replica.
          } else {
            if (outCollision && fLocal < fLocalMax) {
              fLocal += 1
            } else 
	      loopAgain
          }
        } else {
          cur = o // Keep descending.
        }
      } else 
        loopAgain
    }
    out
  }

  def nodeOk(o: Node, nodeStatusMap: Map[String, Float], x: Int) = true
}

// -----------------------------------------------------------

trait Step {
  def doStep(x: Int, t: NodeTree, working: Seq[Node], nodeStatusMap: Map[String, Float]): Seq[Node]
}

case class StepTake(nodeName: String) {
  def doStep(x: Int, t: NodeTree, working: Seq[Node], nodeStatusMap: Map[String, Float]): Seq[Node] =
    t.findNode(nodeName).toList
}

case class StepChooseFirstN(numReplicas: Int, kind: String) {
  def doStep(x: Int, t: NodeTree, working: Seq[Node], nodeStatusMap: Map[String, Float]): Seq[Node] =
    working.flatMap(w => t.choose(x, numReplicas, kind, 0, w, nodeStatusMap))
}

case class StepChooseIndependent(numReplicas: Int, kind: String) {
  def doStep(x: Int, t: NodeTree, working: Seq[Node], nodeStatusMap: Map[String, Float]): Seq[Node] =
    working.flatMap(w => t.choose(x, numReplicas, kind, 0, w, nodeStatusMap))
}
