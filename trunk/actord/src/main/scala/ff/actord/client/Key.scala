/**
 * CRUSH algorithm:
 * http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 */
package ff.actord.client

case class Bucket

class Crush {
  /**
   * Put item a into working vector.
   */
  def crushTake(a: Bucket) = List(a)

  /**
   * Select n items of type t.
   */
  def crushSelect(n: Int, t: String, work: List[Bucket]) = {
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

