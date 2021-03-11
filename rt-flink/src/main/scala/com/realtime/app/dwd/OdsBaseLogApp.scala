package com.realtime.app.dwd

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit


// 从 kafka 读取ods层 用户行为日志数据
object OdsBaseLogApp {
  //定义用户行为主题信息
  val TOPIC_START = "dwd_start_log"
  val TOPIC_PAGE = "dwd_page_log"
  val TOPIC_DISPLAY = "dwd_display_log"

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


    //TODO 从指定Kafka主题读取数据
    //指定消费者配置信息
    val groupId = "ods_dwd_base_log_app"
    val topic = "ods_base_log"
    val kafkaSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(groupId, topic)
    //    val kafkaDS: DataStream[String] = env.addSource(kafkaSource)
    val kafkaDS: DataStream[String] = env.addSource(kafkaSource)

    // 转换为json对象
    val jsonObjectDS: DataStream[JSONObject] = kafkaDS.map(new MapFunction[String, JSONObject]() {
      @throws[Exception]
      override def map(value: String) = {
        val jsonObject: JSONObject = JSON.parseObject(value)
        jsonObject
      }
    })

    println(jsonObjectDS)

    // TODO ********************************************* 开始处理逻辑 *********************************************
    // TODO 2. 用户行为日志 识别新老顾客 按照mid进行分组
    val midKeyedDS: KeyedStream[JSONObject, String] = jsonObjectDS.keyBy(data =>
      data.getJSONObject("common").getString("mid"))

    //校验采集到的数据是新老访客
    val midWithNewFlagDS: DataStream[JSONObject] = midKeyedDS.map(
      new RichMapFunction[JSONObject, JSONObject]() {
        //声明第一次访问日期的状态
        private var firstVisitDataState: ValueState[String] = null
        //声明日期数据格式化对象
        private var simpleDateFormat: SimpleDateFormat = null

        @throws[Exception]
        override def open(parameters: Configuration) = {
          //初始化数据
          firstVisitDataState = getRuntimeContext.getState(new ValueStateDescriptor[String]("newMidDateState", classOf[String]))
          simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        }

        @throws[Exception]
        override def map(jsonObj: JSONObject) = {
          //打印数据
          println(jsonObj)
          //获取访问标记   0表示老访客  1表示新访客
          var isNew: String = jsonObj.getJSONObject("common").getString("is_new")
          //获取数据中的时间戳
          val ts: lang.Long = jsonObj.getLong("ts")

          //判断标记如果为"1",则继续校验数据
          if ("1".equals(isNew)) {
            //获取新访客状态
            val newMidDate: String = firstVisitDataState.value()
            //获取当前数据访问日期
            val tsDate: String = simpleDateFormat.format(new Date(ts))

            //如果新访客状态不为空,说明该设备已访问过 则将访问标记置为"0"
            if (newMidDate != null && (newMidDate.length != 0)) {
              if (!newMidDate.equals(tsDate)) {
                isNew = "0"
                jsonObj.getJSONObject("common").put("is_new", isNew)
              }
            } else {
              //如果复检后，该设备的确没有访问过，那么更新状态为当前日期
              firstVisitDataState.update(tsDate)
            }
          }
          //返回确认过新老访客的json数据
          jsonObj
        }
      }
    )

    //打印测试
    midWithNewFlagDS.print

    // TODO 3.利用侧输出流实现数据拆分
    //定义启动和曝光数据的侧输出流标签
    val startTag: OutputTag[String] = new OutputTag[String]("start")
    val displayTag: OutputTag[String] = new OutputTag[String]("displayTag")

    // 根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
    val pageDStream: DataStream[String] = midWithNewFlagDS.process(
      new ProcessFunction[JSONObject, String] {
        override def processElement(jsonObj: JSONObject, ctx: ProcessFunction[JSONObject, String]#Context, out: Collector[String]) = {
          // 获取数据中的启动相关字段
          val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
          // 将数据转换为字符串，准备向流中输出
          val dataStr: String = jsonObj.toString()
          // 如果是启动日志，输出到启动侧输出流
          if (startJsonObj != null && startJsonObj.size() > 0) {
            ctx.output(startTag, dataStr)
          } else {
            // 非启动日志,则为页面日志或者曝光日志(携带页面信息)
            println("PageString:" + dataStr)
            // 获取数据中的曝光数据,如果不为空,则将每条曝光数据取出输出到曝光日志侧输出流
            val displays: JSONArray = jsonObj.getJSONArray("displayTag")
            if (displays != null && displays.size() > 0) {
              val iter: util.Iterator[AnyRef] = displays.iterator()
              while (iter.hasNext) { // 遍历JSONArray
                // 获取页面id
                val pageId: String = jsonObj.getJSONObject("page").getString("page_id")
                val displayJsonObj: String = iter.next().toString
                // 给每条曝光信息添加上pageId
                JSON.parseObject(displayJsonObj).put("page_id", pageId)
                //将曝光数据输出到测输出流
                ctx.output(displayTag, displayJsonObj)
              }

            } else {
              //将页面数据输出到主流，
              out.collect(dataStr)
            }
          }
        }
      })

    // 获取侧输出流
    val startDStream: DataStream[String] = pageDStream.getSideOutput(startTag)
    val displayDStream: DataStream[String] = pageDStream.getSideOutput(displayTag)

    // 打印测试
    pageDStream.print("page")
    startDStream.print("start")
    displayDStream.print("display")

    // TODO 4.将数据输出到kafka不同的主题中
    val startSink: FlinkKafkaProducer[String] = KafkaUtil.getKafkaSink(TOPIC_START)
    val pageSink: FlinkKafkaProducer[String] = KafkaUtil.getKafkaSink(TOPIC_PAGE)
    val displaySink: FlinkKafkaProducer[String] = KafkaUtil.getKafkaSink(TOPIC_DISPLAY)

    startDStream.addSink(startSink)
    pageDStream.addSink(pageSink)
    displayDStream.addSink(displaySink)

    env.execute("dwd_base_log Job")
  }

}
