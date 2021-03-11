package com.realtime.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, datastream}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.time.Time
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.util.Collector

import java.util
//import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.time.Time
import org.apache.flink.cep._
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
/*
需求：访客跳出情况判断
这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。
用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。

 */
object UserJumpDetailApp {
  def main(args: Array[String]): Unit = {
    // TODO ======================================== Flink配置 ========================================
    // 创建Flink流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4) // 设置并行度 这里和kafka分区数保持一致

    //  设置状态后端
    // 1. 创建一个rocksdb的状态后端，参数是 ck存储的文件系统的路径 依赖需要单独导入
    val rocksDBStateBackend: StateBackend = new RocksDBStateBackend("hdfs://hadoop102:9820/flink1.12/statebackend/rocksDB/"+this.getClass.getName)
    env.setStateBackend(rocksDBStateBackend)
    //    System.setProperty("HADOOP_USER_NAME","root")

    // 2. 配合checkpoint使用，所以一般是开启checkpoint 设置精准一次性保证（默认）
    // 每5000ms开始一次checkpoint
    env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE) // 开启ck，设置周期 每两个 barrier产生的间隔 一般不设置精准一次CheckpointingMode.EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointTimeout(60000) // ck执行1分钟超时
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 异步ck，同时有几个ck开启
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false) // 默认为 false，表示从 ck恢复；true，从savepoint恢复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // ck的最大失败次数

    // 设置重启策略 最大失败次数  衡量失败次数的是时间段 间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // 构建EnvironmentSettings 并指定Blink Planner
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================
    // TODO 1.从kafka的dwd_page_log主题中读取页面日志
    val sourceTopic: String = "dwd_page_log"
    val groupId: String = "userJumpDetailApp"
    val sinkTopic: String = "dwm_user_jump_detail"

    // 从kafka中读取数据
    val dataStream: DataStream[String] = env.addSource(KafkaUtil.getKafkaSource(sourceTopic, groupId))

    // 利用测试数据验证 并行度设为1
/*    val dataStream: DataStream[String] = env.fromElements(
      "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" + "\"home\"},\"ts\":15000} ",
      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" + "\"detail\"},\"ts\":30000} ")
    println(dataStream)*/

    // 对读取的数据进行结构的转换
    val jsonObjStream: DataStream[JSONObject] = dataStream.map(jsonDS => JSON.parseObject(jsonDS))

    // TODO 2.指定事件时间字段
    val jsonObjWithEtDstream: DataStream[JSONObject] = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[JSONObject].withTimestampAssigner(
      new SerializableTimestampAssigner[JSONObject]() {
      override def extractTimestamp(jsonObject: JSONObject, recordTimestamp: Long) = jsonObject.getLong("ts")
    }))

    // TODO 3.根据日志数据的mid进行分组
    val jsonObjStringKeyedStream: KeyedStream[JSONObject, String] = jsonObjWithEtDstream.keyBy(jsonObj => jsonObj.getJSONObject("common").getString("mid"))

    // TODO 4.配置CEP表达式
    val myPattern: Pattern[JSONObject, JSONObject] = Pattern.begin("GoIn").where(
      new SimpleCondition[JSONObject] {
        override def filter(jsonObj: JSONObject): Boolean = {
          val lastPageId: String = jsonObj.getJSONObject("page").getString("last_page_id")
          println("first in :" + lastPageId)
          if (lastPageId == null || (lastPageId.length == 0)) {
            return true
          }
          return false
        }
      }
    ).next("next").where(
      new SimpleCondition[JSONObject] {
        override def filter(jsonObj: JSONObject): Boolean = {
          val pageId: String = jsonObj.getJSONObject("page").getString("page_id")
          println("first in :" + pageId)
          if (pageId != null && pageId.length > 0) {
            return true
          }
          return false
        }
      }
    ).within(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(10000))

    // TODO 5.根据表达式筛选流
    val patternedStream: PatternStream[JSONObject] = CEP.pattern(jsonObjStringKeyedStream,myPattern)

    // TODO 6.提取命中的数据
    val timeoutTag: OutputTag[String] = new OutputTag[String]("timeout")
    val filteredStream: SingleOutputStreamOperator[String] = patternedStream.flatSelect(timeoutTag,
      new PatternFlatTimeoutFunction[JSONObject, String] {
        override def timeout(pattern: util.Map[String, util.List[JSONObject]], timeoutTimestamp: Long, out: Collector[String]) = {
          val objectsList: util.List[JSONObject] = pattern.get("GoIn")
          // 这里进入out的数据都被timeoutTag标记
          for (jsonObj <- objectsList) {
            out.collect(jsonObj.toJSONString)
          }
        }
      },
      new PatternFlatSelectFunction[JSONObject, String] {
        override def flatSelect(pattern: util.Map[String, util.List[JSONObject]], out: Collector[String]) = {
          // 因为不超时的事件不提取，所以这里不写代码
        }
      }
    )
    // 通过SideOutput侧输出流输出超时数据
    val jumpDstream: datastream.DataStream[String] = filteredStream.getSideOutput(timeoutTag)
    jumpDstream.print("jump::")

    // TODO 7.将跳出数据写回到kafka的DWM层
    jumpDstream.addSink(KafkaUtil.getKafkaSink(sinkTopic))

    env.execute(this.getClass.getName+"访客跳出情况判断 Job")
  }
}
