/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

import scala.collection._

trait Node {
  def name: String
  def kind: String // Ex: process, server, shelf, rack, cabinet, row, datacenter, region, universe.
  def info: String // For app-specific information associated with the node, like "host:port" info.
  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node
}

case class LeafNode(name: String, kind: String, info: String) extends Node {
  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node = null
}

trait BucketNode extends Node {
  def bucketType: String

  def child(i: Int): Node
  def childCount: Int
  def childWeight(i: Int): Float

  val sumOfWeights: Float = {
    var x: Float = 0.0F
    for (i <- 0 until childCount)
      x += childWeight(i)
    x
  }

  def hash32(x: Int): Int = 0 // TODO
  def hash32_2(name: String, x: Int): Int = 0 // TODO
  def hash32_2(x: Int, name: String): Int = 0 // TODO
  def hash32_4(x: Int, c: Node, r: Int, name: String): Int = 0 // TODO
}

case class UniformBucket(name: String, kind: String, info: String, children: Seq[Node],
                         uniformChildWeight: Float) 
  extends BucketNode {
  def bucketType: String = "uniform"

  def child(i: Int): Node = children(i)
  def childCount: Int = children.length
  def childWeight(i: Int): Float = uniformChildWeight

  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node = {
    val n = childCount

    val firstN = true
    val r = { if (firstN || rep >= n)
                fTotal + rep
              else if (n % numReplicas == 0)
                fLocal * (rep + 1)
              else
                fLocal * rep }

    val o = hash32_2(x, name) & 0xffff
    val p = primes(hash32_2(name, x).toInt % n)
    val s = (x + o + (r + 1) * p) % n
    children(s)
  }

  val primes: Seq[Int] = {
    val n = childCount
    val a = new Array[Int](n)
    var x = n + 1 + (hash32(n) % (3 * n)) // Need big number.
    x |= 1                                // That is odd.
    var i = 0
    while (i < n) {
       var j = 2
       while (((j * j) <= x) && 
              ((x % j) != 0)) {
         if ((j * j) > x) {
           a(i) = x
           i += 1
         }
         x += 2
         j += 1
       }
    }
    a
  }
}

case class ListBucket(name: String, kind: String, info: String, 
                      children: Seq[Node], childWeights: Seq[Float]) 
  extends BucketNode {
  def bucketType: String = "list"

  def child(i: Int): Node = children(i)
  def childCount: Int = children.length
  def childWeight(i: Int): Float = childWeights(i)

  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node = {
    val n = childCount

    val firstN = true
    val r = { if (firstN)
                fTotal
              else
                fLocal * rep }

    for (i <- 0 until n) {
      val c = child(i)
      var w = hash32_4(x, c, r, name)
      w = w & 0xffff
      w = (w.toFloat * sumOfWeights).toInt
      w = w >> 16
      if (w < childWeight(i))
        return c
    }
    null
  }
}

case class TreeBucket(name: String, kind: String, info: String, 
                      children: Seq[Node], childWeights: Seq[Float]) 
  extends BucketNode {
  def bucketType: String = "tree"

  def child(i: Int): Node = children(i)
  def childCount: Int = children.length
  def childWeight(i: Int): Float = childWeights(i)

  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node = 
    null // TODO
}

case class StrawBucket(name: String, kind: String, info: String, 
                       children: Seq[Node], childWeights: Seq[Float]) 
  extends BucketNode {
  def bucketType: String = "straw"

  def child(i: Int): Node = children(i)
  def childCount: Int = children.length
  def childWeight(i: Int): Float = childWeights(i)

  def chooseChild(x: Int, rep: Int, fTotal: Int, fLocal: Int, numReplicas: Int, firstN: Boolean): Node = 
    null // TODO

  val straws = Nil
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
        case b: BucketNode => 
          for (i <- 0 until b.childCount)
            visitNodes(p, b.child(i))
	case _ => 
      }
    }

  val mapNodes = new mutable.HashMap[String, Node] // Keyed by node's name.

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
             nodeStatusMap: Map[String, Float],
             firstN: Boolean): Seq[Node] = 
      choose(x, numReplicas, kind, startRep, startNode, nodeStatusMap, firstN, 
             MAX_TOTAL_RETRY, MAX_LOCAL_RETRY)

  def choose(x: Int, numReplicas: Int, kind: String, 
             startRep: Int,
             startNode: Node, // TODO: Utilize startRep.
             nodeStatusMap: Map[String, Float],
             firstN: Boolean,
             fTotalMax: Int, 
             fLocalMax: Int): Seq[Node] = {
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
      val o = cur.chooseChild(x, rep, fTotal, fLocal, numReplicas, firstN)
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

  def nodeOk(o: Node, nodeStatusMap: Map[String, Float], x: Int) = true // TODO
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
    working.flatMap(w => t.choose(x, numReplicas, kind, 0, w, nodeStatusMap, true))
}

case class StepChooseIndependent(numReplicas: Int, kind: String) {
  def doStep(x: Int, t: NodeTree, working: Seq[Node], nodeStatusMap: Map[String, Float]): Seq[Node] =
    working.flatMap(w => t.choose(x, numReplicas, kind, 0, w, nodeStatusMap, false))
}

// -----------------------------------------------------------

/*
  UniformBucket("root", 1.0, "root", null, 
    UniformBucket("r_pst_001", 1.0, "region", null, 
      UniformBucket("dc_001", 1.0, "datacenter", null, 
        LeafNode("mc00001", "mc", "192.0.0.1:11211") ::
        LeafNode("mc00002", "mc", "192.0.0.2:11211") ::
        LeafNode("mc00003", "mc", "192.0.0.3:11211") ::
        Nil, 
        1.0) :: 
      Nil,
      1.0) ::
    Nil,
    1.0)
*/