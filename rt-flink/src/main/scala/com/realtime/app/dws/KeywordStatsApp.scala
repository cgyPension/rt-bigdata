package com.realtime.app.dws

import com.realtime.app.udf.KeywordUDTF
import com.realtime.bean.KeywordStats
import com.realtime.conf.GmallConstant
import com.realtime.utils.clickhouse.ClickHouseUtil
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// 搜索关键词计算
object KeywordStatsApp {
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
    tEnv.createTemporarySystemFunction("ik_analyze", classOf[KeywordUDTF.type])

    //TODO 3.将数据源定义为动态表
    val groupId: String = "keyword_stats_app"
    val pageViewSourceTopic: String = "dwd_page_log"

    tEnv.executeSql("CREATE TABLE page_view " +
      "(common MAP<STRING,STRING>, " +
      "page MAP<STRING,STRING>,ts BIGINT, " +
      "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
      "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
      "WITH ("+ KafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")")

    //TODO 4.过滤数据
    val fullwordView: Table = tEnv.sqlQuery("select page['item'] fullword ," +
      "rowtime from page_view  " +
      "where page['page_id']='good_list' " +
      "and page['item'] IS NOT NULL ")

    //TODO 5.利用udtf将数据拆分
    val keywordView: Table = tEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
      " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)")

    //TODO 6.根据各个关键词出现次数进行ct
    val keywordStatsSearch: Table = tEnv.sqlQuery("select keyword,count(*) ct, '"
      + GmallConstant.KEYWORD_SEARCH + "' source ," +
      "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
      "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
      "UNIX_TIMESTAMP()*1000 ts from   " + keywordView
      + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword")

    //TODO 7.转换为数据流
    val keywordStatsSearchDataStream: DataStream[KeywordStats] = tEnv.toAppendStream[KeywordStats](keywordStatsSearch)

    //TODO 8.写入到ClickHouse
    keywordStatsSearchDataStream.addSink(
      ClickHouseUtil.getJdbcSink[KeywordStats](
        "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
          " values(?,?,?,?,?,?)"))

    env.execute(this.getClass.getName + "搜索关键词计算主题宽表 Job")
  }
}
