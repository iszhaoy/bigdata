package collection

object TupleDemo {
  def main(args: Array[String]): Unit = {
    val tuple= (1,2,3)
    println(tuple._1)
    println(tuple.productElement(0))


    println("====================")

    // 元组的便利需要用到迭代器
    for (elem <- tuple.productIterator) {
      println(elem)
    }
  }
}
