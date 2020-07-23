package com.iszhaoy.parameter

import org.apache.flink.api.java.utils.ParameterTool
;

/**
 * this is a study project
 *
 * @author iszhaoy
 * @version 1.0
 * @date 2020/7/22 11:01
 */
object ParameterToolDemo {
  def main(args: Array[String]): Unit = {

    // properties （获取配置文件参数）
    val propertiesFilePath:String = getClass.getResource("/conf.properties").getPath
    val propertiesParameter: ParameterTool = ParameterTool.fromPropertiesFile(propertiesFilePath)
    println(propertiesParameter.get("k1"))

    // command （获取命令行参数）
    val commandParameters: ParameterTool = ParameterTool.fromArgs(args)
    println(commandParameters.get("input"))

    // system properoties （可以获取jdk参数等）
    val systemParameters: ParameterTool = ParameterTool.fromSystemProperties()
    println(systemParameters.get("system-input"))

    // 注册全局参数，在其他函数中可以使用 ParameterTool parameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();获取
    //val parameters: Nothing = ParameterTool.fromArgs(args)
    //
    // set up the execution environment
    //val env: Nothing = ExecutionEnvironment.getExecutionEnvironment
    //env.getConfig.setGlobalJobParameters(parameters)

    // 获取参数
    //val parameters: Nothing = // ...
    //parameter.getRequired("input")
    //parameter.get("output", "myDefaultValue")
    //parameter.getLong("expectedCount", -1L)
    //parameter.getNumberOfParameters
  }
}
