package func

import scala.collection.mutable.ArrayBuffer

object Exec01 {
  def main(args: Array[String]): Unit = {

    val list = "abcdefg"

    val arrayBuffer = new ArrayBuffer[Char]()

    list.foldLeft(arrayBuffer)((arrayBuffer,c) => {arrayBuffer.append(c);arrayBuffer})

    println(arrayBuffer)

  }

}
