package function

object FuncitonDemo1 {
  def main(args: Array[String]): Unit = {
    println(getRes(1, 2, ' '))
  }

  // 定义一个函数/方法
  def getRes(x: Int, y: Int, oper: Char) = {
    if (oper == '+') {
      x + y
    } else if (oper == '-') {
      x - y
    } else {
      // 返回null
      null
    }
  }

}
