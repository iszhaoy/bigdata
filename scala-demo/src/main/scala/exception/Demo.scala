package exception

object Demo {
  def main(args: Array[String]): Unit = {
    try {
      val i = 1;
      val j = 0
      i / j
    } catch {
      case ex:Exception =>
        println("Exception")
      case ex:ArithmeticException =>
        println("ArithmeticException")

    }finally {
      println("finally")
    }
    println("ok")
  }
}
