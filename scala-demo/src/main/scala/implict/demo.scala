package implict

object demo {
  def main(args: Array[String]): Unit = {

    implicit def f1(i: Double): Int = {
      i.toInt
    }

    val i: Int = 1.7


  }
}
