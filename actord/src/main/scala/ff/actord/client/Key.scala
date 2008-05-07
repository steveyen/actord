/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

trait Node {
  def ident: Long
  def weight: Float
  def kind: String // Ex: process, server, shelf, rack, cabinent, row, datacenter, region.
  def info: String
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node
}

case class LeafNode(ident: Long, weight: Float, kind: String, info: String) extends Node {
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = this
}

trait BucketNode extends Node {
  def children: Seq[Node]
  def bucketType: String
}

case class ListBucket(ident: Long, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "list"
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class TreeBucket(ident: Long, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "tree"
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class UniformBucket(ident: Long, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "uniform"
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

case class StrawBucket(ident: Long, weight: Float, kind: String, info: String, children: Seq[Node]) 
  extends BucketNode {
  def bucketType: String = "straw"
  def choose(x: Int, r: Int, h: Int, fTotal: Int, fLocal: Int, numReplicas: Int): Node = children(0)
}

trait Step

class NodeTree(root: Node, steps: Seq[Step]) { 
  /**
   * Choose numReplicas distinct Nodes of a certain kind.
   */
  def choose(x: Int, numReplicas: Int, kind: String, inNodes: Seq[Node], loadMap: Map[Long, Float]): Seq[Node] = {
    for (rep <- 0 until numReplicas) {
      var fTotal = 0
      var fLocal = 0
      var in = inNodes(0)

      val h = 1 // ? what is h?  maybe a hash func?

      var skipRep = false
      var retryRep = false
      while (true) {
        val r = rep
        val out = in.choose(x, r, h, fTotal, fLocal, numReplicas)
        if (out.kind == kind) {
          1
        }
      }
    }
    Nil
  }
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

