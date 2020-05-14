package exec

object exec01 {
  def main(args: Array[String]): Unit = {
    printfuc(6)
  }

  def printfuc(n: Int): Unit = {
    var start = 1
    var blank: Int = n - 1
    for (i <- 1 to n) {
      // 打印空格
      for (j <- 0 to blank) {
        print(" ")
      }
      for (k <- 1 to start) {
        print("*")
      }
      blank -= 1
      start += 2
      println()
    }

  }
}
