package com.realtime.app.dwd

import com.alibaba.fastjson.JSON.parseObject
import com.alibaba.fastjson.{JSON, JSONObject}
import com.realtime.bean.{TableProcess, TableProcess2}
import com.realtime.utils.kafka.KafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, datastream}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import com.realtime.utils.flink.TableProcessFunction
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import com.realtime.utils.flink.DimSink
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.util.concurrent.TimeUnit
import scala.::

// 从Kafka中读取ods层业务数据 并进行处理 发送到DWD层
object OdsBaseDbApp {
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

    // TODO 1.接收Kafka数据，过滤空值数据
    val topic: String = "ods_base_db_m"
    val groupId: String = "ods_base_group"

    val kafkaSource: FlinkKafkaConsumer[String] = KafkaUtil.getKafkaSource(topic, groupId)
    val jsonDstream: DataStream[String] = env.addSource(kafkaSource)
    // jsonDstream.print("data json:::::::")

    // 对数据进行结构的转换   String->JSONObject
    val jsonStream: DataStream[JSONObject] = jsonDstream.map(jsonStr => JSON.parseObject(jsonStr))

    // 过滤为空或者 长度不足的数据
    val filteredDstream: DataStream[JSONObject] = jsonStream.filter(jsonObject => {
      val flag: Boolean = jsonObject.getString("table") != null && jsonObject.getJSONObject("data") != null && jsonObject.getString("data").length > 3
      flag
    })

    filteredDstream.print("json::::::::")


    // 根据MySQL的配置表，动态进行分流
    /*
      flink_realtime库中创建配置表table_process

      CREATE TABLE `table_process` (
  `source_table` varchar(200) NOT NULL COMMENT '来源表',
  `operate_type` varchar(200) NOT NULL COMMENT '操作类型 insert,update,delete',
   `sink_type` varchar(200) DEFAULT NULL COMMENT '输出类型 hbase kafka',
  `sink_table` varchar(200) DEFAULT NULL COMMENT '输出表(主题)',
  `sink_columns` varchar(2000) DEFAULT NULL COMMENT '输出字段',
  `sink_pk` varchar(200) DEFAULT NULL COMMENT '主键字段',
  `sink_extend` varchar(200) DEFAULT NULL COMMENT '建表扩展',
  PRIMARY KEY (`source_table`,`operate_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
     */
    // TODO 2.动态分流  事实表放入主流，作为DWD层；维度表放入侧输出流
    // 定义输出到Hbase的侧输出流标签
    val hbaseTaq: OutputTag[JSONObject] = new OutputTag[JSONObject](TableProcess2.asInstanceOf[TableProcess2].SINK_TYPE_HBASE)
    // 使用自定义ProcessFunction 进行分流处理
    val kafkaDS: DataStream[JSONObject] = filteredDstream.process(new TableProcessFunction(hbaseTaq))

    // 获取侧输出流，即将通过Phoenix写到Hbase的数据
    val hbaseDS: DataStream[JSONObject] = kafkaDS.getSideOutput(hbaseTaq)

    // TODO 3.将侧输出流数据写入HBase(Phoenix)
    hbaseDS.print("hbase::::")
    hbaseDS.addSink(new DimSink)

    // TODO 4将主流数据写入Kafka
    val kafkaSink: FlinkKafkaProducer[JSONObject] = KafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema[JSONObject] {
      override def open(context: SerializationSchema.InitializationContext) = {
        println("启动Kafka Sink")
      }
      // 从每条数据得到该条数据应送往的主题名
      override def serialize(jsonObject: JSONObject, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val topic: String = jsonObject.getString("sink_table")
        val dataJsonObj: JSONObject = jsonObject.getJSONObject("data")
        new ProducerRecord(topic, dataJsonObj.toJSONString().getBytes())
      }
    })

    kafkaDS.print("kafka ::::")
    kafkaDS.addSink(kafkaSink)
    env.execute("dwd_base_db Job")

  }
}
