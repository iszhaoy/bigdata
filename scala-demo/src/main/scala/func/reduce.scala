package func

object reduce {
  def main(args: Array[String]): Unit = {
    val result = (1 to 3).reduce(_-_)
    println(result)
  }
}
