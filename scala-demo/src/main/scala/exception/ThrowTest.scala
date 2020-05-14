package exception

object ThrowTest {
  def main(args: Array[String]): Unit = {
    try {
      val res = test()
      print(res.toString)
    } catch {
      case ex: Exception => println("test~~~~~~~:" + ex.getMessage())
    }

    println("ok") // 不会执行
  }

  def test(): Nothing = {
    throw new Exception("123123test")
  }
}
