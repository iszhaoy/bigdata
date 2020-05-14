package fundetails

object fundetails02 {
  def main(args: Array[String]): Unit = {
    println(test2())
  }

  // 如果写了return，返回值类型就不能省略，不会自动自断
  def test1 = {
    // return 1
    1
  }

  // 如果返回值什么都不写，表示该函数没有返回值
   def test2() = 1 + 2


}
