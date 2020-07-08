package com.iszhaoy.mysql

import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector


/**
 * this is a study project
 *
 * @author iszhaoy
 * @date 2020/7/6 9:53
 * @version 1.0
 */
object MysqlTwoCommitSinkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    // 使用checkpoint,每5秒保存一次
    env.enableCheckpointing(5000)
    val checkPointPath = new Path("file:///tmp/flink-checkpoint-2020-07-08")

    // 配置FsStateBackend
    val fsStateBackend = new FsStateBackend(checkPointPath)
    env.setStateBackend(fsStateBackend)

    // 配置RocksDBbackend
    //val tmpDir = "D:/tmp/rocksdb/data"
    //val rocksDBBackend: RocksDBStateBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE)
    //val config = new Configuration()
    ////TIMER分为HEAP(默认，性能更好)和RocksDB(扩展好)
    //config.setString(RocksDBOptions.TIMER_SERVICE_FACTORY, RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString)
    //rocksDBBackend.configure(config, getClass.getClassLoader)
    ////rocksDBBackend.setDbStoragePath(tmpDir)
    //env.setStateBackend(rocksDBBackend.asInstanceOf[StateBackend])

    // 设置模式为 exactly-once (default)
    // 一般对于超低延迟的应用(大概几毫秒)可以使用CheckpointingMode.AT_LEAST_ONCE，其他大部分应用使用CheckpointingMode.EXACTLY_ONCE就可以
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 检查点必须在一分钟内完成，否则将被丢弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 确保在检查点之间有500毫秒的进度
    // 用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，当指定这个参数时，maxConcurrentCheckpoints的值为1
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 同一时间只允许一个检查点执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 任务被取消之后，保留ck, 也可以配置删除ck
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置可容忍的检查点次数，如果检查点错误，任务会失败
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)

    val socketSource: DataStream[String] = env.socketTextStream("hadoop01", 9999)
    socketSource.print("data")
    socketSource
      .map(x => (1, x))
      .keyBy(_._1)
      .countWindow(3)
      .process(new ProcessWindowFunction[(Int, String),java.util.List[String], Int, GlobalWindow] {
        override def process(key: Int, context: Context, elements: Iterable[(Int, String)], out: Collector[java.util.List[String]])
        : Unit = {
          val list = new java.util.ArrayList[String]()
          for (e <- elements) {
            list.add(e._2)
          }
          out.collect(list)
          println("触发窗口->" + list)
        }
      })
      .addSink(new MySqlTwoPhaseCommitSink)

    env.execute("job")
  }
}


