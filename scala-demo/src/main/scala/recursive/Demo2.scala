package recursive

object Demo2 {
  def main(args: Array[String]): Unit = {
    println(fibo(4))
    println(f(2))

    println(eat(1))
  }

  def fibo(n: Int): Int = {
    if (n == 1 || n == 2) return 1
    fibo(n - 2) + fibo(n - 1)
  }

  def f(n: Int): Int = {
    if (n == 1) return 3
    2 * f(n - 1) + 1
  }

  //猴子吃桃子
  // day[10] == 1
  // day[9] == (day[10] + 1) * 2
  // day[8] == (day[9] + 1) * 2
  def eat(day: Int): Int = {
    if (day == 10) {
      1
    } else {
      (eat(day + 1) + 1) * 2
    }
  }
}
