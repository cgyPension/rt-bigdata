package com.realtime.app.dws

import com.realtime.bean.ProvinceStats
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import com.realtime.utils.clickhouse.ClickHouseUtil
import scala.collection.JavaConversions._

// 实现地区主题宽表计算
// FlinkSQL
object ProvinceStatsSqlApp {
  def main(args: Array[String]): Unit = {
    // TODO ======================================== Flink配置 ========================================
    // 创建Flink流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4) // 设置并行度 这里和kafka分区数保持一致

    //  设置状态后端
    // 1. 创建一个rocksdb的状态后端，参数是 ck存储的文件系统的路径 依赖需要单独导入
    val rocksDBStateBackend: StateBackend = new RocksDBStateBackend("hdfs://hadoop102:9820/flink1.12/statebackend/rocksDB/" + this.getClass.getName)
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
    //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // 构建EnvironmentSettings 并指定Blink Planner
    //val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()

    // 构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================
    //TODO 2.把数据源定义为动态表
    // WATERMARK FOR rowtime AS rowtime是把某个虚拟字段设定为EVENT_TIME
    val groupId: String = "province_stats"
    val orderWideTopic: String = "dwm_order_wide"

    tEnv.executeSql(
      """
        |CREATE TABLE ORDER_WIDE (
        |		     province_id BIGINT,
        |        province_name STRING,province_area_code STRING ,
        |		     province_iso_code STRING,
        |		     province_3166_2_code STRING,
        |        order_id STRING,
        |        split_total_amount DOUBLE,
        |        create_time STRING,
        |		     rowtime AS TO_TIMESTAMP(create_time) ,
        |        WATERMARK FOR  rowtime  AS rowtime)
        |		WITH (
        |""".stripMargin + KafkaUtil.getKafkaDDL(orderWideTopic, groupId+)+")" )

    //TODO 3.聚合计算
    val provinceStateTable: Table = tEnv.sqlQuery("select " +
      "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
      "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
      " province_id,province_name,province_area_code area_code," +
      "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
      "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
      "UNIX_TIMESTAMP()*1000 ts " +
      " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
      " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ")

    //TODO 4.转换为数据流
    val provinceStatsDataStream: DataStream[ProvinceStats] = tEnv.toAppendStream(provinceStateTable)

    //TODO 5.写入到lickHouse//TODO 5.写入到lickHouse
    provinceStatsDataStream.addSink(ClickHouseUtil.getJdbcSink[ProvinceStats]("insert into  province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"))

    env.execute(this.getClass.getName + "地区主题宽表计算 Job")
  }
}
