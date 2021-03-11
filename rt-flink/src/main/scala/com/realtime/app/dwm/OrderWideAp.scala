package com.realtime.app.dwm

import com.alibaba.fastjson.JSONObject
import com.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.realtime.utils.DimAsyncFunction
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util.Date
//import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import com.alibaba.fastjson.JSON

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit


// 处理订单和订单明细数据形成订单宽表
object OrderWideAp {
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
    // TODO 1.从Kafka的dwd层接收订单和订单明细数据
    val orderInfoSourceTopic: String = "dwd_order_info"
    val orderDetailSourceTopic: String = "dwd_order_detail"
    val orderWideSinkTopic: String = "dwm_order_wide"
    val groupId: String = "order_wide_group"

    // 从Kafka中读取数据
    val sourceOrderInfo: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId)
    val sourceOrderDetail: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId)
    val orderInfojsonDStream: DataStream[String] = env.addSource(sourceOrderInfo)
    val orderDetailJsonDStream: DataStream[String] = env.addSource(sourceOrderDetail)

    // 对读取的数据进行结构的转换 这里直接在样例类里就可以转
/*    val orderInfoDStream: DataStream[OrderInfo] = orderInfojsonDStream.map(mapper = new RichMapFunction[String, OrderInfo] {
      var simpleDateFormat: SimpleDateFormat = null

      override def open(parameters: Configuration) = {
        super.open(parameters)
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      }

      override def map(jsonString: String) = {
        val orderInfo: OrderInfo = jsonString.asInstanceOf[OrderInfo]
        orderInfo.create_ts = simpleDateFormat.parse(orderInfo.create_time).getTime()
        orderInfo
      }
    })

    val orderDetailDStream: DataStream[OrderDetail] = orderDetailJsonDStream.map(mapper = new RichMapFunction[String, OrderDetail] {
      var simpleDateFormat: SimpleDateFormat = null
      override def open(parameters: Configuration) = {
        super.open(parameters)
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      }
      override def map(jsonString: String) = {
        val orderDetail: OrderDetail = jsonString.asInstanceOf[OrderDetail]
        orderDetail.create_ts = simpleDateFormat.parse(orderDetail.create_time).getTime()
        orderDetail
      }
    })*/

    val orderInfoDStream: DataStream[OrderInfo] = orderInfojsonDStream.map(mapper => mapper.asInstanceOf[OrderInfo])
    val orderDetailDStream: DataStream[OrderDetail] = orderDetailJsonDStream.map(mapper => mapper.asInstanceOf[OrderDetail])

    orderInfoDStream.print("orderInfo::::")
    orderDetailDStream.print("orderDetail::::")

    /**
     * 订单和订单明细关联(双流join)
     * 在flink中的流join大体分为两种，一种是基于时间窗口的join（Time Windowed Join），比如join、coGroup等。另一种是基于状态缓存的join（Temporal Table Join），比如intervalJoin。
     * 这里选用intervalJoin，因为相比较窗口join，intervalJoin使用更简单，而且避免了应匹配的数据处于不同窗口的问题。intervalJoin目前只有一个问题，就是还不支持left join。
     * 但是我们这里是订单主表与订单从表之间的关联不需要left join，所以intervalJoin是较好的选择。
     */

    // TODO 2.设定事件时间水位
    val orderInfoWithEventTimeDstream: DataStream[OrderInfo] = orderInfoDStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[OrderInfo].withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo]() {
      override def extractTimestamp(orderInfo: OrderInfo, recordTimestamp: Long) = orderInfo.create_ts
    }))

    val orderDetailWithEventTimeDstream: DataStream[OrderDetail] = orderDetailDStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[OrderDetail].withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail]() {
      override def extractTimestamp(orderDetail: OrderDetail, recordTimestamp: Long) = orderDetail.create_ts
    }))

    // TODO 3.设定关联的key
    val orderInfoKeyedDstream: KeyedStream[OrderInfo, Long] = orderInfoWithEventTimeDstream.keyBy(orderInfo => orderInfo.id)
    val orderDetailKeyedDstream: KeyedStream[OrderDetail, Long] = orderDetailWithEventTimeDstream.keyBy(orderDetail => orderDetail.order_id)
    // TODO 4.订单和订单明细关联 intervalJoin 这里设置了正负5秒，以防止在业务系统中主表与从表保存的时间差
    val orderWideDstream: DataStream[OrderWide] = orderInfoKeyedDstream.intervalJoin(orderDetailKeyedDstream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide] {
        override def processElement(left: OrderInfo, right: OrderDetail, ctx: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context, out: Collector[OrderWide]) = {
          out.collect(OrderWide().mergeOrderInfo(left).mergeOrderDetail(right))
        }
      })
    orderWideDstream.print("joined ::")

    // TODO 5.关联用户维度
    // DimAsyncFunction 核心的类是AsyncDataStream，这个类有两个方法一个是有序等待（orderedWait），一个是无序等待（unorderedWait）
    val orderWideWithUserDstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideDstream, new DimAsyncFunction[OrderWide]("DIM_USER_INFO") {
      /**
       * 需要实现如何把结果装配给数据流对象
       * @param t  数据流对象
       * @param jsonObject   异步查询结果
       * @throws Exception
       */
      override def join(orderWide: OrderWide, jsonObject: JSONObject) = {
        val formattor: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val birthday: String = jsonObject.getString("BIRTHDAY")
        val date: Date = formattor.parse(birthday)

        val curTs: Long = System.currentTimeMillis
        val betweenMs: Long = curTs - date.getTime
        val ageLong: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L
        orderWide.user_age = ageLong.toInt
        orderWide.user_gender = jsonObject.getString("GENDER")
      }
      /**
       * 需要实现如何从流中对象获取主键
       * @param t  数据流对象
       */
      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.user_id)
      }
    }, 60, TimeUnit.SECONDS)

    orderWideWithUserDstream.print("dim join user:")

    //TODO 6.关联省市维度
    val orderWideWithProvinceDstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithUserDstream, new DimAsyncFunction[OrderWide]("DIM_BASE_PROVINCE") {
      override def join(orderWide: OrderWide, jsonObject: JSONObject) = {
        orderWide.province_name = jsonObject.getString("NAME")
        orderWide.province_3166_2_code = jsonObject.getString("ISO_3166_2")
        orderWide.province_iso_code = jsonObject.getString("ISO_CODE")
        orderWide.province_area_code = jsonObject.getString("AREA_CODE")
      }

      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.province_id)
      }
    }, 60, TimeUnit.SECONDS)


    // TODO 7.关联SKU维度
    val orderWideWithSkuDstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithProvinceDstream, new DimAsyncFunction[OrderWide]("DIM_SKU_INFO") {
      @throws[Exception]
      def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
        orderWide.sku_name=jsonObject.getString("SKU_NAME")
        orderWide.category3_id=jsonObject.getLong("CATEGORY3_ID")
        orderWide.spu_id=jsonObject.getLong("SPU_ID")
        orderWide.tm_id=jsonObject.getLong("TM_ID")
      }

      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.sku_id)
      }
    }, 60, TimeUnit.SECONDS)

    // TODO 8.关联SPU商品维度
    val orderWideWithSpuDstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithSkuDstream, new DimAsyncFunction[OrderWide]("DIM_SPU_INFO") {
      @throws[Exception]
      def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
        orderWide.spu_name=jsonObject.getString("SPU_NAME")
      }

      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.spu_id)
      }
    }, 60, TimeUnit.SECONDS)

    //TODO 9.关联品类维度
    val orderWideWithCategory3Dstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithSpuDstream, new DimAsyncFunction[OrderWide]("DIM_BASE_CATEGORY3") {
      @throws[Exception]
      def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
        orderWide.category3_name=jsonObject.getString("NAME")
      }

      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.category3_id)
      }
    }, 60, TimeUnit.SECONDS)

    //TODO 10.关联品牌维度
    val orderWideWithTmDstream:  DataStream[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithCategory3Dstream, new DimAsyncFunction[OrderWide]("DIM_BASE_TRADEMARK") {
      @throws[Exception]
      def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
        orderWide.tm_name=jsonObject.getString("TM_NAME")
      }

      override def getKey(orderWide: OrderWide) = {
        String.valueOf(orderWide.tm_id)
      }
    }, 60, TimeUnit.SECONDS)

    // TODO 11.将订单和订单明细Join之后以及维度关联的宽表写到Kafka的dwm层
    orderWideWithTmDstream.map(orderWide => JSON.toJSONString(orderWide))
      .addSink(KafkaUtil.getKafkaSink(orderWideSinkTopic))

    env.execute(this.getClass.getName+"订单宽表 Job")
  }
}
