package recursive

object Demo1 {
  def main(args: Array[String]): Unit = {
    test1(4)
    println("---------------")
    test2(4)
  }

  def test1(n: Int) {
    if (n > 2) {
      test1(n - 1)
    }
    println(n)
  }

  def test2(n: Int) {
    if (n > 2) {
      test2(n - 1)
    } else {
      println(n)
    }
  }
}
