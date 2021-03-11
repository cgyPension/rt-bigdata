package com.realtime.ods.demo

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.util.concurrent.TimeUnit

/*
   upsert-kafka connector 既可以作为 source 使用，也可以作为 sink 使用

 必须指定Key和Value的序列化格式，其中Key是通过PRIMARY KEY指定的。

Primary Key约束
Upsert Kafka 工作在 upsert 模式（FLIP-149）下。当我们创建表时，需要在 DDL 中定义主键。具有相同key的数据，会存在相同的分区中。在 changlog source 上定义主键意味着在物化后的 changelog 上主键具有唯一性。定义的主键将决定哪些字段出现在 Kafka 消息的 key 中。

一致性保障
默认情况下，如果启用 checkpoint，Upsert Kafka sink 会保证至少一次将数据插入 Kafka topic。

这意味着，Flink 可以将具有相同 key 的重复记录写入 Kafka topic。但由于该连接器以 upsert 的模式工作，该连接器作为 source 读入时，可以确保具有相同主键值下仅最后一条消息会生效。因此，upsert-kafka 连接器可以像 HBase sink 一样实现幂等写入。

分区水位线
Flink 支持根据 Upsert Kafka 的 每个分区的数据特性发送相应的 watermark。当使用这个特性的时候，watermark 是在 Kafka consumer 内部生成的。 合并每个分区生成的 watermark 的方式和 streaming shuffle 的方式是一致的(单个分区的输入取最大值，多个分区的输入取最小值)。 数据源产生的 watermark 是取决于该 consumer 负责的所有分区中当前最小的 watermark。如果该 consumer 负责的部分分区是空闲的，那么整体的 watermark 并不会前进。在这种情况下，可以通过设置合适的 table.exec.source.idle-timeout 来缓解这个问题。

数据类型
Upsert Kafka 用字节bytes存储消息的 key 和 value，因此没有 schema 或数据类型。消息按格式进行序列化和反序列化，例如：csv、json、avro。不同的序列化格式所提供的数据类型有所不同，因此需要根据使用的序列化格式进行确定表字段的数据类型是否与该序列化类型提供的数据类型兼容。
 */

object FlinkKafkaSource_02 extends Serializable {

  def main(args: Array[String]): Unit = {

    // TODO ======================================== Flink配置 ========================================
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置重启策略 最大失败次数  衡量失败次数的是时间段 间隔
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));


    // TODO 设置状态后端
    // 1. 创建一个rocksdb的状态后端，参数是 ck存储的文件系统的路径 依赖需要单独导入
    /*    val rocksDBStateBackend: StateBackend = new RocksDBStateBackend("hdfs://hadoop102:9820/flink1.12/statebackend/rocksDB")
        env.setStateBackend(rocksDBStateBackend)*/
    // 2. 配合checkpoint使用，所以一般是开启checkpoint
    env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE) // 开启ck，设置周期 每两个 barrier产生的间隔
    env.getCheckpointConfig.setCheckpointTimeout(30000L) // ck执行多久超时
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 异步ck，同时有几个ck开启
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false) // 默认为 false，表示从 ck恢复；true，从savepoint恢复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // ck的最大失败次数

    //构建EnvironmentSettings 并指定Blink Planner
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================

    def ods_fact_user_ippv =
      """
        |CREATE TABLE ods_fact_user_ippv (
        |    user_id      STRING,
        |    client_ip    STRING,
        |    client_info  STRING,
        |    pagecode     STRING,
        |    access_time  TIMESTAMP,
        |    dt           STRING,
        |    WATERMARK FOR access_time AS access_time - INTERVAL '5' SECOND  -- 定义watermark
        |) WITH (
        |   'connector' = 'kafka',
        |    'topic' = 'user_ippv',
        |    'scan.startup.mode' = 'earliest-offset',
        |    'properties.group.id' = 'group1',
        |    'properties.bootstrap.servers' = 'xxx:9092',
        |    'format' = 'json',
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin

    def result_total_pvuv_min =
      """
        |CREATE TABLE result_total_pvuv_min (
        |    do_date     STRING,     -- 统计日期
        |    do_min      STRING,      -- 统计分钟
        |    pv          BIGINT,     -- 点击量
        |    uv          BIGINT,     -- 一天内同个访客多次访问仅计算一个UV
        |    currenttime TIMESTAMP,  -- 当前时间
        |    PRIMARY KEY (do_date, do_min) NOT ENFORCED
        |) WITH (
        |  'connector' = 'upsert-kafka',
        |  'topic' = 'result_total_pvuv_min',
        |  'properties.bootstrap.servers' = 'xxx:9092',
        |  'key.json.ignore-parse-errors' = 'true',
        |  'value.json.fail-on-missing-field' = 'false',
        |  'key.format' = 'json',
        |  'value.format' = 'json',
        |  'value.fields-include' = 'ALL'
        |)
        |""".stripMargin


    tEnv.executeSql(ods_fact_user_ippv) // 创建表
    tEnv.executeSql(result_total_pvuv_min) // 创建表

    def view_total_pvuv_min =
      """
        |-- 创建视图
        |CREATE VIEW view_total_pvuv_min AS
        |SELECT
        |     dt AS do_date,                    -- 时间分区
        |     count (client_ip) AS pv,          -- 客户端的IP
        |     count (DISTINCT client_ip) AS uv, -- 客户端去重
        |     max(access_time) AS access_time   -- 请求的时间
        |FROM
        |    source_ods_fact_user_ippv
        |GROUP BY dt
        |""".stripMargin

    tEnv.executeSql(view_total_pvuv_min)

    def result =
      """
        |-- 将每分钟的pv/uv统计结果写入kafka upsert表
        |INSERT INTO result_total_pvuv_min
        |SELECT
        |  do_date,
        |  cast(DATE_FORMAT (access_time,'HH:mm') AS STRING) AS do_min,-- 分钟级别的时间
        |  pv,
        |  uv,
        |  CURRENT_TIMESTAMP AS currenttime
        |from
        |  view_total_pvuv_min
        |""".stripMargin

    tEnv.sqlUpdate(view_total_pvuv_min)


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
