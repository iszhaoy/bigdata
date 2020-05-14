package macthtest

object MatchObject {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3)
//    list.collect()

    val squre = Squre(6.0)
    squre match {
      case Squre(n) => println(n)
      case _ => "什么都没有匹配到"
    }

    val str = "tom,bob,ailis"
    str match {
      case Names(n1, n2, n3) => {
        println(s"$n1,$n2,$n3")
      }
      case _ => print("None")
    }

  }
}

object Squre {
  def unapply(n: Double): Option[Double] = Option(Math.sqrt(n))

  def apply(n: Double): Double = n * n
}

object Names {
  def unapplySeq(str: String): Option[Seq[String]] = {
    if (str.contains(","))
      Some(str.split(","))
    else None
  }

}