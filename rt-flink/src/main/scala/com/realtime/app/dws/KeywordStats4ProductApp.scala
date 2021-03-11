package com.realtime.app.dws

import com.realtime.app.udf.{KeywordProductC2RUDTF, KeywordUDTF}
import com.realtime.bean.KeywordStats
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import com.realtime.utils.clickhouse.ClickHouseUtil

// 商品行为关键字主题宽表计算
object KeywordStats4ProductApp {
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
    // TODO 2.注册自定义函数
    // 注意json格式的要定义为Map对象
    tEnv.createTemporarySystemFunction("keywordProductC2R", classOf[KeywordProductC2RUDTF.type])

    //TODO 3.将数据源定义为动态表
    val groupId: String = "keyword_stats_app"
    val productStatsSourceTopic: String = "dws_product_stats"
    
    tEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
      "click_ct BIGINT," +
      "cart_ct BIGINT," +
      "order_ct BIGINT ," +
      "stt STRING,edt STRING ) " +
      "  WITH (" + KafkaUtil.getKafkaDDL(productStatsSourceTopic, groupId) + ")")

    //TODO 6.聚合计数
    val keywordStatsProduct: Table = tEnv.sqlQuery("select keyword,ct,source, " +
      "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss')  stt," +
      "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
      "UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
      "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
      "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)")

    //TODO 7.转换为数据流
    val keywordStatsProductDataStream: DataStream[KeywordStats] = tEnv.toAppendStream[KeywordStats](keywordStatsProduct)

    keywordStatsProductDataStream.print

    //TODO 8.写入到ClickHouse
    keywordStatsProductDataStream.addSink(ClickHouseUtil.getJdbcSink[KeywordStats](
      "insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts)  " + "values(?,?,?,?,?,?)")
    )

    env.execute(this.getClass.getName + "商品行为关键字主题宽表 Job")
  }
}
