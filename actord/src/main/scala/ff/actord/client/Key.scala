/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

import scala.collection._

trait Node {
  def name: String
  def weight: Float
  def kind: String // Ex: process, server, shelf, rack, cabinent, row, datacenter, region, universe.
  def info: String // For app-specific information associated with the node, like "host:port" info.
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node
}

case class LeafNode(name: String, weight: Float, kind: String, info: String) extends Node {
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = null
}

trait BucketNode extends Node {
  def children: Seq[Node]
  def bucketType: String
}

case class ListBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "list"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class TreeBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "tree"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class UniformBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "uniform"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class StrawBucket(name: String, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "straw"
  def chooseChild(x: Int, r: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

class NodeTree(root: Node, steps: Seq[Step]) { 
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
             loadMap: Map[String, Float]): Seq[Node] = 
      choose(x, numReplicas, kind, startRep, startNode, loadMap, MAX_TOTAL_RETRY, MAX_LOCAL_RETRY)

  def choose(x: Int, numReplicas: Int, kind: String, 
             startRep: Int,
             startNode: Node,
             loadMap: Map[String, Float],
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
              nodeOk(o, loadMap, x)) {
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

  def nodeOk(o: Node, loadMap: Map[String, Float], x: Int) = true
}

trait Step {
  def doStep(t: NodeTree, working: Seq[Node]): Seq[Node]
}

case class StepTake(nodeName: String) {
  def doStep(t: NodeTree, working: Seq[Node]): Seq[Node] = {
    t.findNode(nodeName).toList
  }
}

case class StepChooseFirstN(numReplicas: Int) {
  def doStep(t: NodeTree, working: Seq[Node]): Seq[Node] = {
    Nil
  }
}

case class StepChooseIndependent(numReplicas: Int) {
  def doStep(t: NodeTree, working: Seq[Node]): Seq[Node] = {
    Nil
  }
}
