package com.iszhaoy.customer

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


case class CustomerReceiver(hostName: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  // 读数据并将数据发送给Spark

  def receive(): Unit = {
    var socket: Socket = null
    //定义一个变量，用来接收端口传过来的数据
    var input: String = null
    var reader:BufferedReader = null
    try {
      socket = new Socket(hostName, port)

      //创建一个BufferedReader用于读取端口传来的数据
      reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

      //读取数据
      input = reader.readLine()

      //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
      while (!isStopped() && input != null) {
        store(input)
        input = reader.readLine()
      }

    } catch {
      case e: Exception =>
        print(e)
        //重启任务
        //跳出循环则关闭资源
        reader.close()
        socket.close()
        restart("restart")
    }

  }

  // 关闭时调用的方法
  override def onStop(): Unit = {

  }
}
