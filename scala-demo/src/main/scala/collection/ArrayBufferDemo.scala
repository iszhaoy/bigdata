package collection

import scala.collection.mutable.ArrayBuffer

object ArrayBufferDemo {
  def main(args: Array[String]): Unit = {
    val arr = ArrayBuffer[Int]()
    println(arr.hashCode())
    arr.append(2)
    arr.append(3)
    println(arr.hashCode())
    println(arr)
    arr.remove(1)
    for (elem <- arr) {
      println(elem)
    }

  }
}
