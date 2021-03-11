package com.realtime.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import com.realtime.bean.{OrderInfo, ProductStats, VisitorStats}
import com.realtime.utils.clickhouse.ClickHouseUtil
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date
import scala.collection.JavaConversions._

/**
 * Desc: 访客主题宽表计算
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 * 进行关联  这是一个fulljoin
 * 可以考虑使用flinksql 完成
 */
object VisitorStatsApp {
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
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================
    // TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
    val groupId: String = "visitor_stats_app"
    val pageViewSourceTopic: String = "dwd_page_log"
    val uniqueVisitSourceTopic: String = "dwm_unique_visit"
    val userJumpDetailSourceTopic: String = "dwm_user_jump_detail"

    val pageViewSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId)
    val uniqueVisitSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId)
    val userJumpSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId)

    val pageViewDStream: DataStream[String] = env.addSource(pageViewSource)
    val uniqueVisitDStream: DataStream[String] = env.addSource(uniqueVisitSource)
    val userJumpDStream: DataStream[String] = env.addSource(userJumpSource)

    //TODO 2.对读取的流进行结构转换
    //2.1 转换pv流
    val pageViewStatsDstream: DataStream[VisitorStats] = pageViewDStream.map(json => {
      val jsonObj: JSONObject = JSON.parseObject(json)
      new VisitorStats(
        "", "",
        jsonObj.getJSONObject("common").getString("vc"),
        jsonObj.getJSONObject("common").getString("ch"),
        jsonObj.getJSONObject("common").getString("ar"),
        jsonObj.getJSONObject("common").getString("is_new"),
        0L, 1L, 0L, 0L,
        jsonObj.getJSONObject("page").getLong("during_time"),
        jsonObj.getLong("ts")
      )
    })

    //2.2转换uv流
    val uniqueVisitStatsDstream: DataStream[VisitorStats] = uniqueVisitDStream.map(json => {
      val jsonObj: JSONObject = JSON.parseObject(json)
      new VisitorStats(
        "", "",
        jsonObj.getJSONObject("common").getString("vc"),
        jsonObj.getJSONObject("common").getString("ch"),
        jsonObj.getJSONObject("common").getString("ar"),
        jsonObj.getJSONObject("common").getString("is_new"),
        1L, 0L, 0L, 0L, 0L,
        jsonObj.getLong("ts")
      )
    })

    //2.3 转换sv流
    val sessionVisitDstream: DataStream[VisitorStats] = pageViewDStream.process(
      new ProcessFunction[String, VisitorStats] {
        override def processElement(jsonStr: String, ctx: ProcessFunction[String, VisitorStats]#Context, out: Collector[VisitorStats]) = {
          val jsonObj: JSONObject = JSON.parseObject(jsonStr)
          val lastPageId: String = jsonObj.getJSONObject("page").getString("last_page_id")
          if (lastPageId == null || lastPageId.length() == 0) {
            val visitorStats: VisitorStats = new VisitorStats(
              "", "",
              jsonObj.getJSONObject("common").getString("vc"),
              jsonObj.getJSONObject("common").getString("ch"),
              jsonObj.getJSONObject("common").getString("ar"),
              jsonObj.getJSONObject("common").getString("is_new"),
              0L, 0L, 1L, 0L, 0L,
              jsonObj.getLong("ts")
            )
            out.collect(visitorStats)
          }
        }
      }
    )

    //2.4 转换跳转流
    val userJumpStatDstream: DataStream[VisitorStats] = userJumpDStream.map(json => {
      val jsonObj: JSONObject = JSON.parseObject(json)
      new VisitorStats(
        "", "",
        jsonObj.getJSONObject("common").getString("vc"),
        jsonObj.getJSONObject("common").getString("ch"),
        jsonObj.getJSONObject("common").getString("ar"),
        jsonObj.getJSONObject("common").getString("is_new"),
        0L, 0L, 0L, 1L, 0L,
        jsonObj.getLong("ts")
      )
    })

    // TODO 3.将四条流合并起来
    val unionDetailDstream: DataStream[VisitorStats] = uniqueVisitStatsDstream.union(
      pageViewStatsDstream, sessionVisitDstream, userJumpStatDstream
    )

    // TODO 4.设置水位线 因为涉及开窗聚合，所以要设定事件时间及水位线
    //val visitorStatsWithWatermarkDstream: DataStream[VisitorStats] = unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))[VisitorStats].withTimestampAssigner((VisitorStats, ts) => VisitorStats.ts))
    val visitorStatsWithWatermarkDstream: DataStream[VisitorStats] = unionDetailDstream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))[VisitorStats].withTimestampAssigner(new SerializableTimestampAssigner[VisitorStats]() {
      override def extractTimestamp(visitorStats: VisitorStats, recordTimestamp: Long) = visitorStats.ts
    }))


    // TODO 5.分组 选取四个维度作为key , 使用Tuple4组合
    val visitorStatsTuple4KeyedStream: KeyedStream[VisitorStats, (String, String, String, String)] = visitorStatsWithWatermarkDstream.keyBy(new KeySelector[VisitorStats, Tuple4[String, String, String, String]] {
      override def getKey(visitorStats: VisitorStats) = {
        new Tuple4[String, String, String, String](visitorStats.vc, visitorStats.ch, visitorStats.ar, visitorStats.is_new)
      }
    })

    // TODO 6.开窗
    val windowStream: WindowedStream[VisitorStats, (String, String, String, String), TimeWindow] = visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))

    // TODO 7.Reduce聚合统计
    val visitorStatsDstream: DataStream[VisitorStats] = windowStream.reduce(function = new ReduceFunction[VisitorStats] {
      override def reduce(stats1: VisitorStats, stats2: VisitorStats) = {
        // 把度量数据两两相加
        stats1.pv_ct = stats1.pv_ct + stats2.pv_ct
        stats1.uv_ct = stats1.uv_ct + stats2.uv_ct
        stats1.uj_ct = stats1.uj_ct + stats2.uj_ct
        stats1.sv_ct = stats1.sv_ct + stats2.sv_ct
        stats1.dur_sum = stats1.dur_sum + stats2.dur_sum
        stats1
      },
      new ProcessWindowFunction[VisitorStats, VisitorStats, (String, String, String, String), TimeWindow] {
        override def process(key: (String, String, String, String), context: Context, visitorStatsIn: Iterable[VisitorStats], out: Collector[VisitorStats]): Unit = {
          // 补时间字段
          val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          for (visitorStats <- visitorStatsIn) {
            val startDate: String = simpleDateFormat.format(new Date(context.window.getStart))
            val endDate: String = simpleDateFormat.format(new Date(context.window.getEnd))
            visitorStats.stt = startDate
            visitorStats.edt = endDate
            out.collect(visitorStats)
          }
        }
      }
    })
    visitorStatsDstream.print("reduce:")

    // TODO 8.向ClickHouse中写入数据
    visitorStatsDstream.addSink(
      ClickHouseUtil.getJdbcSink(
        "insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"
      )
    )

    env.execute(this.getClass.getName + "访客主题宽表计算 Job")
  }
}
