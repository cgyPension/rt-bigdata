package com.realtime.ods.demo

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

import java.util.concurrent.TimeUnit


/**
hive-exec内置的guava版本是 19.0
hadoop3.1.3内置的guava是 27.0
这两个冲突了，要把 hive exec的guava注掉，但是他没法明确注掉

https://www.jianshu.com/p/f076a4f66527

我自己也编译了一版，你自个编译一下就行
 */
object FlinkKafkaSource_01 extends Serializable{
  def main(args: Array[String]): Unit = {

    // TODO ======================================== Flink配置 ========================================
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO 设置状态后端
    // 1. 创建一个rocksdb的状态后端，参数是 ck存储的文件系统的路径 依赖需要单独导入
    val rocksDBStateBackend: StateBackend = new RocksDBStateBackend("hdfs://hadoop102:9820/flink1.12/statebackend/rocksDB/"+this.getClass.getName)
    env.setStateBackend(rocksDBStateBackend)

    // 2. 配合checkpoint使用，所以一般是开启checkpoint
    env.enableCheckpointing(3000L) // 开启ck，设置周期 每两个 barrier产生的间隔 一般不设置精准一次CheckpointingMode.EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointTimeout(30000L) // ck执行多久超时
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 异步ck，同时有几个ck开启
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false) // 默认为 false，表示从 ck恢复；true，从savepoint恢复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // ck的最大失败次数

    // 设置重启策略 最大失败次数  衡量失败次数的是时间段 间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    //构建EnvironmentSettings 并指定Blink Planner
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================

    /**
     *{"user_id": "543462", "item_id":"1715", "category_id": "1464116", "behavior": "pv", "ts": "2017-11-26 01:00:00"}
     */
    def user_test =
      """
        |CREATE TABLE user_test (
        |user_id BIGINT,
        |item_id BIGINT,
        |category_id BIGINT,
        |behavior STRING,
        |`record_time` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp',
        |ts TIMESTAMP(3) ) WITH (
        |'connector' = 'kafka',
        |'topic' = 'user_test',
        |'properties.group.id' = 'local-sql-test',
        |'scan.startup.mode' = 'latest-offset',
        |'properties.bootstrap.servers' = 'hadoop102:9092',
        |'format' = 'json' ,
        |'json.timestamp-format.standard' = 'SQL',
        |'scan.topic-partition-discovery.interval'='10000',
        |'json.fail-on-missing-field' = 'false',
        |'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin


    tEnv.executeSql(user_test) // 创建表

    //执行查询
    val table: Table = tEnv.sqlQuery("select * from user_test ")


    //转回DataStream并输出 rowDataStream
    val rowDataStream: DataStream[Row] = tEnv.toAppendStream[Row](table)

    rowDataStream.map(new MapFunction[Row, Row] {
      override def map(value: Row): Row = {
        println(value)
        return value
      }
    }).print(this.getClass.getName + "==").setParallelism(1)

    //任务启动，这行必不可少！
    try {
      env.execute(this.getClass.getName)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }


  }


}
