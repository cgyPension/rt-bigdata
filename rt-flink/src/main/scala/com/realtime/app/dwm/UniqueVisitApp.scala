package com.realtime.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.lang
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import scala.util.control.Breaks._

/*
  访客UV的计算
  从kafka的dwd_page_log主题接收数据
 */
object UniqueVisitApp {
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

    //TODO 1.从Kafka中读取数据
    val groupId: String = "unique_visit_app"
    val sourceTopic: String = "dwd_page_log"
    val sinkTopic: String = "dwm_unique_visit"

    // 读取kafka数据
    val source: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(sourceTopic, groupId)
    val jsonStream: DataStream[String] = env.addSource(source)

    // 对读取的数据进行结构的转换
    val jsonObjStream: DataStream[JSONObject] = jsonStream.map(jsonDS => JSON.parseObject(jsonDS))

    // TODO 2.核心的过滤代码
    // 按照设备id进行分组
    val keyByWithMidstream: KeyedStream[JSONObject, String] = jsonObjStream.keyBy(jsonObj => jsonObj.getJSONObject("common").getString("mid"))
    val filteredJsonObjDstream: DataStream[JSONObject] = keyByWithMidstream.filter(new RichFilterFunction[JSONObject] {
      // 定义状态用于存放最后访问的
      var lastVisitDateState: ValueState[String] = null
      // 日期格式
      var simpleDateFormat: SimpleDateFormat = null

      // 初始化状态以及时间格式
      override def open(parameters: Configuration) = {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        if (lastVisitDateState == null) {
          val lastViewDateStateDescriptor: ValueStateDescriptor[String] = new ValueStateDescriptor("lastViewDateState", classOf[String])
          // 因为统计的是当日UV，也就是日活，所有为状态设置失效时间
          val stateTtlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build()
          //默认值 表明当状态创建或每次写入时都会更新时间戳
          //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          //默认值  一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态
          //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build()
          lastViewDateStateDescriptor.enableTimeToLive(stateTtlConfig)
          lastVisitDateState = getRuntimeContext().getState(lastViewDateStateDescriptor)
        }
      }

      // 检查当前页面是否有上页标识，如果有说明该次访问一定不是当日首次
      override def filter(jsonObject: JSONObject): Boolean = {
        val lastPageId: String = jsonObject.getJSONObject("page").getString("last_page_id")
        if (lastPageId != null && lastPageId.length() > 0) {
          return false
        }
        val ts: lang.Long = jsonObject.getLong("ts")
        val logDate: String = simpleDateFormat.format(ts)
        val lastViewDate: String = lastVisitDateState.value()

        if (lastViewDate != null && lastViewDate.length() > 0 && logDate.equals(lastViewDate)) {
          println("已访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
          return false
        } else {
          println("未访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
          lastVisitDateState.update(logDate);
          return true
        }
      }
    }).uid("uvFilter")

    val dataJsonStringDstream: DataStream[String] = filteredJsonObjDstream.map(jsonObj => jsonObj.toJSONString)

    // dataJsonStringStream.print("uv")
    // TODO 3.将过滤处理后的UV写入到Kafka的dwm主题
    dataJsonStringDstream.addSink(KafkaUtil.getKafkaSink(sinkTopic))

    env.execute("UniqueVisitApp独立访客 Job")
  }
}
