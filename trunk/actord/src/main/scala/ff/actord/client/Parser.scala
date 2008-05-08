import scala.util.parsing.combinator.syntactical._

object OrderDSL extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",")
  lexical.reserved += ("buy", "sell", "shares", "at", "max", "min", "for", "trading", "account")

  def instr = trans ~ account_spec

  def trans = "(" ~> repsep(trans_spec, ",") <~ ")"

  def trans_spec = buy_sell ~ buy_sell_instr

  def account_spec = "for" ~> "trading" ~> "account" ~> stringLit

  def buy_sell = ("buy" | "sell")

  def buy_sell_instr = security_spec ~ price_spec

  def security_spec = numericLit ~ ident ~ "shares"

  def price_spec = "at" ~ ("min" | "max") ~ numericLit

/*
  def scala2JavaList(sl: List[LineItem]): java.util.List[LineItem] = {
    var jl = new java.util.ArrayList[LineItem]()
    sl.foreach(jl.add(_))
    jl
  }

  lexical.delimiters ++= List("(", ")", ",")
  lexical.reserved += ("buy", "sell", "shares", "at", "max", "min", "for", "trading", "account")

  def instr: Parser[ClientOrder] =
    trans ~ account_spec ^^ { case t ~ a => new ClientOrder(scala2JavaList(t), a) }

  def trans: Parser[List[LineItem]] =
    "(" ~> repsep(trans_spec, ",") <~ ")" ^^ { (ts: List[LineItem]) => ts }

  def trans_spec: Parser[LineItem] =
    buy_sell ~ buy_sell_instr ^^ { case bs ~ bsi => new LineItem(bsi._1._2, bsi._1._1, bs, bsi._2) }

  def account_spec =
    "for" ~> "trading" ~> "account" ~> stringLit ^^ {case s => s}

  def buy_sell: Parser[ClientOrder.BuySell] =
    ("buy" | "sell") ^^ { case "buy" => ClientOrder.BuySell.BUY
                          case "sell" => ClientOrder.BuySell.SELL }

  def buy_sell_instr: Parser[((Int, String), Int)] =
    security_spec ~ price_spec ^^ { case s ~ p => (s, p) }

  def security_spec: Parser[(Int, String)] =
    numericLit ~ ident ~ "shares" ^^ { case n ~ a ~ "shares" => (n.toInt, a) }

  def price_spec: Parser[Int] =
    "at" ~ ("min" | "max") ~ numericLit ^^ { case "at" ~ s ~ n => n.toInt }
*/
}


/*
def doMatch() {
  val dsl =
    "(buy 100 IBM shares at max 45, sell 40 Sun shares at min 24," +
     "buy 25 CISCO shares at max 56) for trading account \"A1234\""

  instr(new lexical.Scanner(dsl)) match {
    case Success(ord, _) => processOrder(ord) // ord is a ClientOrder
    case Failure(msg, _) => println(msg)
    case Error(msg, _) => println(msg)
  }
*/

/*
nodes: {
  cabinent: {
    name: "cab0001",
    kind: "uniform",
    contains: {
      shelf: {
        name: "shelf0001",
        kind: "list",
        contains: {
          server: {
            name: "mc0001",
            info: "198.0.0.1:11211"
          }
        }
      }
    }
  }
},
steps: [
  { take: "root" },
  { choose: "cabinent", n: 3, alg: "firstN" },
  { choose: "shelves",  n: 2 },
  { choose: "process",  n: 2 }
]
*/
