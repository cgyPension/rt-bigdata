package com.realtime.app.dws

import com.realtime.bean.{OrderInfo, OrderWide, PaymentWide, ProductStats}
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import com.realtime.utils.{DateTimeUtil, DimAsyncFunction}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.realtime.utils.clickhouse.ClickHouseUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Date}
import scala.collection.JavaConversions._

// 商品主题
// 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
// 注意：一定要匹配两个数据生成模拟器的日期，否则窗口无法匹配上
object ProductStatsApp {
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
    //TODO 1.从Kafka中获取数据流
    val groupId: String = "product_stats_app"

    val pageViewSourceTopic: String = "dwd_page_log"
    val orderWideSourceTopic: String = "dwm_order_wide"
    val paymentWideSourceTopic: String = "dwm_payment_wide"
    val cartInfoSourceTopic: String = "dwd_cart_info"
    val favorInfoSourceTopic: String = "dwd_favor_info"
    val refundInfoSourceTopic: String = "dwd_order_refund_info"
    val commentInfoSourceTopic: String = "dwd_comment_info"

    val pageViewSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId)
    val orderWideSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(orderWideSourceTopic, groupId)
    val paymentWideSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId)
    val favorInfoSourceSouce: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId)
    val cartInfoSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId)
    val refundInfoSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId)
    val commentInfoSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId)

    val pageViewDStream: DataStream[String] = env.addSource(pageViewSource)
    val favorInfoDStream: DataStream[String] = env.addSource(favorInfoSourceSouce)
    val orderWideDStream: DataStream[String] = env.addSource(orderWideSource)
    val paymentWideDStream: DataStream[String] = env.addSource(paymentWideSource)
    val cartInfoDStream: DataStream[String] = env.addSource(cartInfoSource)
    val refundInfoDStream: DataStream[String] = env.addSource(refundInfoSource)
    val commentInfoDStream: DataStream[String] = env.addSource(commentInfoSource)

    // TODO 2.对获取的流数据进行结构的转换
    // 2.1转换曝光及页面流数据
    val pageAndDispStatsDstream: DataStream[ProductStats] = pageViewDStream.process(processFunction = new ProcessFunction[String, ProductStats]() {
      override def processElement(json: String, ctx: ProcessFunction[String, ProductStats]#Context, out: Collector[ProductStats]) = {
        val jsonObj: JSONObject = JSON.parseObject(json)
        val pageJsonObj: JSONObject = jsonObj.getJSONObject("page")
        val pageId: String = pageJsonObj.getString("page_id")
        if (pageId == null) println(jsonObj)
        val ts: Long = jsonObj.getLong("ts")
        if (pageId.equals("good_detail")) {
          val skuId: Long = pageJsonObj.getLong("item")
          val productStats: ProductStats = ProductStats(sku_id = skuId, click_ct = 1L, ts = ts)
          out.collect(productStats)
        }
        val displays: JSONArray = jsonObj.getJSONArray("displays")
        if (displays != null && displays.size() > 0) {
          for (i <- 0 until displays.size()) {
            val display: JSONObject = displays.getJSONObject(i)
            if (display.getString("item_type").equals("sku_id")) {
              val skuId: Long = display.getLong("item")
              val productStats: ProductStats = ProductStats(sku_id = skuId, display_ct = 1L, ts = ts)
              out.collect(productStats)
            }
          }
        }
      }
    })

    // 2.2转换下单流数据
    val orderWideStatsDstream: DataStream[ProductStats] = orderWideDStream.map(json => {
      val orderWide: OrderWide = JSON.parseObject(json, classOf[OrderWide])
      println("orderWide:===" + orderWide)
      val create_time: String = orderWide.create_time
      val ts: Long = DateTimeUtil.toTs(create_time)
      ProductStats(sku_id = orderWide.sku_id, orderIdSet = new util.HashSet(Collections.singleton(orderWide.order_id)),
        order_sku_num = orderWide.sku_num, order_amount = orderWide.split_total_amount, ts = ts)
    })

    // 2.3转换收藏流数据
    val favorStatsDstream: DataStream[ProductStats] = favorInfoDStream.map(json => {
      val favorInfo: JSONObject = JSON.parseObject(json)
      val ts: Long = DateTimeUtil.toTs(favorInfo.getString("create_time"))
      ProductStats(sku_id = favorInfo.getLong("sku_id"), favor_ct = 1L, ts = ts)
    })

    // 2.4转换购物车流数据
    val cartStatsDstream: DataStream[ProductStats] = cartInfoDStream.map(json => {
      val cartInfo: JSONObject = JSON.parseObject(json)
      val ts: Long = DateTimeUtil.toTs(cartInfo.getString("create_time"))
      ProductStats(sku_id = cartInfo.getLong("sku_id"), cart_ct = 1L, ts = ts)
    })

    //2.5转换支付流数据
    val paymentStatsDstream: DataStream[ProductStats] = paymentWideDStream.map(json => {
      val paymentWide: PaymentWide = JSON.parseObject(json, classOf[OrderWide])
      val ts: Long = DateTimeUtil.toTs(paymentWide.payment_create_time)
      ProductStats(sku_id = paymentWide.sku_id, payment_amount = paymentWide.split_total_amount, paidOrderIdSet = new util.HashSet(Collections.singleton(paymentWide.order_id)), ts = ts)
    })

    //2.6转换退款流数据
    val refundStatsDstream: DataStream[ProductStats] = refundInfoDStream.map(json => {
      val refundJsonObj: JSONObject = JSON.parseObject(json)
      val ts: Long = DateTimeUtil.toTs(refundJsonObj.getString("create_time"))
      ProductStats(sku_id = refundJsonObj.getLong("sku_id"), refund_amount = refundJsonObj.getBigDecimal("refund_amount"),
        refundOrderIdSet = new util.HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))),
        ts = ts)
    })

    //2.7转换评价流数据
    val commonInfoStatsDstream: DataStream[ProductStats] = commentInfoDStream.map(json => {
      val commonJsonObj: JSONObject = JSON.parseObject(json)
      val ts: Long = DateTimeUtil.toTs(commonJsonObj.getString("create_time"))
      import com.realtime.conf.GmallConstant
      val goodCt: Long = if (GmallConstant.APPRAISE_GOOD == commonJsonObj.getString("appraise")) 1L
      else 0L
      ProductStats(sku_id = commonJsonObj.getLong("sku_id"), comment_ct = 1L, good_comment_ct = goodCt, ts = ts)
    })

    //TODO 3.把统一的数据结构流合并为一个流
    val productStatDetailDStream: DataStream[ProductStats] = pageAndDispStatsDstream.union(orderWideStatsDstream, cartStatsDstream, paymentStatsDstream, refundStatsDstream, favorStatsDstream, commonInfoStatsDstream)


    //TODO 4.设定事件时间与水位线
/*    val productStatsWithTsStream: DataStream[ProductStats] = productStatDetailDStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[ProductStats].withTimestampAssigner((productStats: ProductStats, recordTimestamp: Long) => {
      productStats.ts
    }))*/
    val productStatsWithTsStream: DataStream[ProductStats] = productStatDetailDStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[ProductStats].withTimestampAssigner(new SerializableTimestampAssigner[ProductStats]() {
      override def extractTimestamp(productStats: ProductStats, recordTimestamp: Long) = productStats.ts
    }))

    //TODO 5.分组、开窗、聚合
    val productStatsDstream: DataStream[ProductStats] = productStatsWithTsStream.keyBy(new KeySelector[ProductStats, Long] {
      //5.1 按照商品id进行分组
      override def getKey(productStats: ProductStats) = {
        productStats.sku_id
      }
    }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      //5.1 按照商品id进行分组
      .reduce(new ReduceFunction[ProductStats] {
        override def reduce(stats1: ProductStats, stats2: ProductStats) = {
          stats1.display_ct = stats1.display_ct + stats2.display_ct
          stats1.click_ct = stats1.click_ct + stats2.click_ct
          stats1.cart_ct = stats1.cart_ct + stats2.cart_ct
          stats1.favor_ct = stats1.favor_ct + stats2.favor_ct
          stats1.order_amount = stats1.order_amount + stats2.order_amount
          stats1.orderIdSet.addAll(stats2.orderIdSet)
          stats1.order_sku_num = stats1.order_sku_num + stats2.order_sku_num
          stats1.payment_amount = stats1.payment_amount + stats2.payment_amount
          stats1.refundOrderIdSet.addAll(stats2.refundOrderIdSet)
          stats1.refund_order_ct = stats1.refundOrderIdSet.size + 0L

          stats1.refund_amount = stats1.refund_amount + stats2.refund_amount
          stats1.paidOrderIdSet.addAll(stats2.paidOrderIdSet)
          stats1.paid_order_ct = stats1.paidOrderIdSet.size() + 0L
          stats1.comment_ct = stats1.comment_ct + stats2.comment_ct
          stats1.good_comment_ct = stats1.good_comment_ct + stats2.good_comment_ct
          stats1
        }
      },
        new WindowFunction[ProductStats, ProductStats, Long, TimeWindow] {
          override def apply(key: Long, window: TimeWindow, productStatsIterable: Iterable[ProductStats], out: Collector[ProductStats]): Unit = {
            val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            for (productStats <- productStatsIterable) {
              productStats.stt = simpleDateFormat.format(window.getStart)
              productStats.edt = simpleDateFormat.format(window.getEnd)
              productStats.ts = new Date().getTime
              out.collect(productStats)
            }
          }
        }
      )

    //TODO 6.补充商品维度信息
    //因为除了下单操作之外，其它操作，只获取到了商品的id，其它维度信息是没有的
    //6.1 补充SKU维度
    val productStatsWithSkuDstream: DataStream[ProductStats] = AsyncDataStream.unorderedWait(productStatsDstream,
      new DimAsyncFunction[ProductStats]("DIM_SKU_INFO") {
        override def join(productStats: ProductStats, jsonObject: JSONObject): Unit = {
          productStats.sku_name = jsonObject.getString("SKU_NAME")
          productStats.sku_price = jsonObject.getBigDecimal("PRICE")
          productStats.category3_id = jsonObject.getLong("CATEGORY3_ID")
          productStats.spu_id = jsonObject.getLong("SPU_ID")
          productStats.tm_id = jsonObject.getLong("TM_ID")
        }

        override def getKey(productStats: ProductStats) = {
          String.valueOf(productStats.sku_id)
        }
      }, 60, TimeUnit.SECONDS)

    //6.2 补充SPU维度
    val productStatsWithSpuDstream: DataStream[ProductStats] = AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
      new DimAsyncFunction[ProductStats]("DIM_SPU_INFO") {
        override def join(productStats: ProductStats, jsonObject: JSONObject): Unit = {
          productStats.spu_name = jsonObject.getString("SPU_NAME")
        }

        override def getKey(productStats: ProductStats) = {
          String.valueOf(productStats.spu_id)
        }
      }, 60, TimeUnit.SECONDS)


    //6.3 补充品类维度
    val productStatsWithCategory3Dstream: DataStream[ProductStats] = AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
      new DimAsyncFunction[ProductStats]("DIM_BASE_CATEGORY3") {
        override def join(productStats: ProductStats, jsonObject: JSONObject): Unit = {
          productStats.category3_name = jsonObject.getString("CATEGORY3_NAME")
        }

        override def getKey(productStats: ProductStats) = {
          String.valueOf(productStats.category3_id)
        }
      }, 60, TimeUnit.SECONDS)

    //6.4 补充品牌维度
    val productStatsWithTmDstream: DataStream[ProductStats] = AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
      new DimAsyncFunction[ProductStats]("DIM_BASE_TRADEMARK") {
        override def join(productStats: ProductStats, jsonObject: JSONObject): Unit = {
          productStats.tm_name = jsonObject.getString("TM_NAME")
        }

        override def getKey(productStats: ProductStats) = {
          String.valueOf(productStats.tm_id)
        }
      }, 60, TimeUnit.SECONDS)

    productStatsWithTmDstream.print("to save")

    //TODO 7.写入到ClickHouse
    productStatsWithTmDstream.addSink(
      ClickHouseUtil.getJdbcSink[ProductStats](
        "insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"))

    //TODO 8.写回到Kafka的dws层
    import com.alibaba.fastjson.JSON
    import com.alibaba.fastjson.serializer.SerializeConfig
    productStatsWithTmDstream.map(productStat => JSON.toJSONString(productStat, new SerializeConfig(true))).addSink(KafkaUtil.getKafkaSink("dws_product_stats"))

    env.execute(this.getClass.getName + "商品主题宽表计算 Job")
  }
}
