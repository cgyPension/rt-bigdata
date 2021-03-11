package com.realtime.ods

import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
object OdsKafkaToFlink {
  def main(args: Array[String]): Unit = {
/*    // 1.创建执行环境 Flink 1.12 默认为EventTime
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // TODO Table API
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 使用官方的老版本
      //      .useBlinkPlanner()  // 使用Blink版本
      .inStreamingMode() // 基于流处理的模式
      //      .inBatchMode()  // 基于批处理的模式
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)





    env.execute()*/

  }
}
