package exception

object ThrowsComment {
  def main(args: Array[String]): Unit = {
    f11()
  }
  @throws(classOf[NumberFormatException])
  def f11() = {
    "abc".toInt
  }
}

