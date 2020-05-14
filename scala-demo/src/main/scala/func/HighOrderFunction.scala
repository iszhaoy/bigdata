package func

object HighOrderFunction {
  def main(args: Array[String]): Unit = {
    println(test(sum, 1.0))
    test2(sayOK)

    val f1: () => Unit = sayOK _
    f1()
  }

  def test2(f: () => Unit): Unit = {

  }

  def sayOK() = {
    println("ok")
  }

  def test(f: Double => Double, num: Double): Double = {
    f(num)
  }

  def sum(num: Double): Double = {
    num + num
  }

}
