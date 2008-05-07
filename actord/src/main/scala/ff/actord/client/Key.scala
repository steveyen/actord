/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

trait Node {
  def name: String
  def weight: Float
  def kind: String // Ex: process, server, shelf, rack, cabinent, row, datacenter, region.
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

trait Step

class NodeTree(root: Node, steps: Seq[Step]) { 
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

class Crush {
  /**
   * Select n items of type t.
   */
  def crushSelect(n: Int, t: String, work: List[Node]) = {
    /*
    var out = Nil // Out output, initially empty.
    for (bucket <- work) {
      for (rep <- 0 until numrep) {
        // keep trying until we get an non-out, non-colliding item 
        var ftotal = 0 // No failures yet.

        var retry_descent = true
        while (retry_descent) {
          retry_descent = false

          var in = bucket // initial bucket
          var flocal = 0 // No failures on this replica.

          var retry_bucket = true
          while (retry_bucket) {
            retry_bucket = false

            var r = rep
            var rprime = r + ftotal // TODO: Alg dependent calc here (first-n, uniform, etc)
            var o = b.c(rprime, x) // Pseudo-randomly choose a nested item
            if (type(o) == t) {
              val collision = out.find(o)
              if (collision || is_out(o) or failed(o) or overload(o, x) {
                ftotal = ftotal + 1
                flocal = flocal + 1
                if (collision && flocal < 3)
                  retry_bucket = true // Retry collisions locally a few times.
                else if (ftotal < 10)
                  retry_descent = true // Otherwise retry descent from i
              } else
                out = out ::: List(o)
                
            } else {
              in = bucket(o) // Continue descent
              retry_bucket = true
            }
          }
        }
      }
    }
    out
    */
  }
  
  def calcPlacementGroup(r: Int, o: String, k: Int) = {
    // m = (2^k) - 1
    // pgid = (r, hash(o) & m)
  }
}

