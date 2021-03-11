package com.realtime.dwd.dm层从hive直接读取.demo

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableResult}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

import java.time.Duration

object $01_FlinkWithHive {
  def main(args: Array[String]): Unit = {

    // 初始化环境 默认的时间语义为EventTime
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(3)
    val tableEnvSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))

    // 连接Hive
    val name = "myhive"
    val defaultDatabase = "mydatabase"
    val hiveConfDir = "/opt/hive-conf"
    val hive_01: HiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("hive_01", hive_01)
    tableEnv.useCatalog("hive_01")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    // 读写hive
    tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka_stream")
    val result: TableResult = tableEnv.executeSql("DROP TABLE IF EXISTS kafka_stream.kafka_source_topic")

     println(result.getJobClient.get.getJobStatus)

    tableEnv.executeSql(
      """
        |CREATE TABLE kafka_stream.kafka_source_topic (
        |  ts BIGINT,
        |  userId BIGINT,
        |  username STRING,
        |  gender STRING,
        |  procTime AS PROCTIME(),
        |  eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),
        |  WATERMARK FOR eventTime AS eventTime - INTERVAL '15' SECOND
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'kafka_source_topic',
        |  'properties.bootstrap.servers' = 'localhost:9092'
        |  'properties.group.id' = 'flink_hive',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
  """.stripMargin
    )
    //其他操作如Hive建表、消费源数据写入Kafka分区等
  }
}
