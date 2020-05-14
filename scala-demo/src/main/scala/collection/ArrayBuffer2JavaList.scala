package collection

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ArrayBuffer2JavaList {
  def main(args: Array[String]): Unit = {
    // 只能传string
    val arr = ArrayBuffer("1", "2", "3")
    import scala.collection.JavaConversions.bufferAsJavaList
    val builder = new ProcessBuilder(arr)
    val arrList = builder.command()
    println(arrList)


    import scala.collection.JavaConversions.asScalaBuffer
    val arr2:mutable.Buffer[String]= arrList
    println(arr2)
  }
}
