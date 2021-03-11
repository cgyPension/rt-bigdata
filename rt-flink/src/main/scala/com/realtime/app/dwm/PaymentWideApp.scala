package com.realtime.app.dwm

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import com.alibaba.fastjson.JSON
import com.realtime.bean.{OrderInfo, OrderWide, PaymentInfo, PaymentWide}
import com.realtime.utils.DateTimeUtil
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

object PaymentWideApp {
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
    //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // 构建EnvironmentSettings 并指定Blink Planner
    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // 构建StreamTableEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // TODO ======================================== 逻辑 ========================================
    //TODO 1.接收数据流
    val groupId: String = "payment_wide_group"
    val paymentInfoSourceTopic: String = "dwd_payment_info"
    val orderWideSourceTopic: String = "dwm_order_wide"
    val paymentWideSinkTopic: String = "dwm_payment_wide"


    //封装Kafka消费者  读取支付流数据
    val paymentInfoSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId)
    val paymentInfojsonDstream: DataStream[String] = env.addSource(paymentInfoSource)
    //对读取的支付数据进行转换
    val paymentInfoDStream: DataStream[PaymentInfo] = paymentInfojsonDstream.map(jsonString => jsonString.asInstanceOf[PaymentInfo])

    //封装Kafka消费者  读取订单宽表流数据
    val orderWideSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(orderWideSourceTopic, groupId)
    val orderWidejsonDstream: DataStream[String] = env.addSource(orderWideSource)
    //对读取的订单宽表数据进行转换
    val orderWideDstream: DataStream[OrderWide] = orderWidejsonDstream.map(jsonString => jsonString.asInstanceOf[OrderWide])

    //设置水位线
    val paymentInfoEventTimeDstream: DataStream[PaymentInfo]  = paymentInfoDStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[PaymentInfo](Duration.ofSeconds(3)).withTimestampAssigner((paymentInfo: PaymentInfo, ts: Long) => DateTimeUtil.toTs(paymentInfo.callback_time)))

    val orderInfoWithEventTimeDstream: DataStream[OrderWide] = orderWideDstream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[OrderWide](Duration.ofSeconds(3)).withTimestampAssigner((orderWide: OrderWide, ts: Long) => DateTimeUtil.toTs(orderWide.create_time)))

    //设置分区键
    val paymentInfoKeyedStream: KeyedStream[PaymentInfo, Long] = paymentInfoEventTimeDstream.keyBy(paymentInfo => paymentInfo.order_id)
    val orderWideKeyedStream: KeyedStream[OrderWide, Long] = orderInfoWithEventTimeDstream.keyBy(orderWide => orderWide.order_id)

    // 关联数据
    val paymentWideDstream: DataStream[PaymentWide] = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
      .between(Time.seconds(-1800), Time.seconds(0))
      .process(new ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide] {
        override def processElement(left: PaymentInfo, right: OrderWide, ctx: ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]#Context, out: Collector[PaymentWide]) = {
          out.collect(PaymentWide().mergePaymentInfo(left).mergeOrderWide(right))
        }
      }).uid("payment_wide_join")

    val paymentWideStringDstream: DataStream[String] = paymentWideDstream.map(paymentWide => JSON.toJSONString(paymentWide))

    paymentWideStringDstream.print("pay:")
    paymentWideStringDstream.addSink(KafkaUtil.getKafkaSink(paymentWideSinkTopic))

    env.execute(this.getClass.getName+"支付宽表 Job")
  }
}
