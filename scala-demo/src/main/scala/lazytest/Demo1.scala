package lazytest

object Demo1 {

  def main(args: Array[String]): Unit = {
    lazy val res = sum(10, 20)
    print(res)
  }

  def sum(i: Int, i1: Int) = {
    println("test")
    i + i1
  }


}
