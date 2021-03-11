package com.realtime.utils.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import com.realtime.conf.Constants._
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

object KafkaUtil {
  // 缺省情况下会采用DEFAULT_TOPIC
  private val DEFAULT_TOPIC = "DEFAULT_DATA"

  // 封装Kafka消费者
  def getKafkaSource(topic: String, groupId: String): FlinkKafkaConsumer[String] = {
    val prop: Properties = new Properties()
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS_PROD)
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
  }


  // 封装Kafka生产者
  def getKafkaSink(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](KAFKA_BROKERS_PROD, topic, new SimpleStringSchema())
  }

  // 封装Kafka生产者  动态指定多个不同主题
  def getKafkaSinkBySchema[T](serializationSchema: KafkaSerializationSchema[T]): FlinkKafkaProducer[T] = {
    val prop: Properties = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS_PROD)
    // 如果15分钟没有更新状态，则超时 默认1分钟
    prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "")
    new FlinkKafkaProducer(DEFAULT_TOPIC, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }

  //拼接Kafka相关属性到DDL//拼接Kafka相关属性到DDL
  def getKafkaDDL(topic: String, groupId: String): String = {
    val ddl: String = "'connector' = 'kafka', " +
      " 'topic' = '"+topic+"',"   +
      " 'properties.bootstrap.servers' = '"+ KAFKA_BROKERS_PROD +"', " +
      " 'properties.group.id' = '"+groupId+ "', " +
      "  'format' = 'json', " +
      "  'scan.startup.mode' = 'latest-offset'  "+
      """
        |'json.timestamp-format.standard' = 'SQL',
        |'scan.topic-partition-discovery.interval'='10000',
        |'json.fail-on-missing-field' = 'false',
        |'json.ignore-parse-errors' = 'true'
        |""".stripMargin
    ddl

  }
}
