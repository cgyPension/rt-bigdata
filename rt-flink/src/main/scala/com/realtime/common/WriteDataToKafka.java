package com.realtime.common;


import com.realtime.conf.ConfigurationManager;
import com.realtime.conf.Constants;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;


public class WriteDataToKafka {
    private static Logger logger = LogManager.getLogger(WriteDataToKafka.class.getName());
    public static void SendKafka(String env, ProducerRecord record) {
        String brokers = GetBrokers(env);
        Properties kafkaProps = InitKafkaParams(brokers);

        // 实例化出producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        try {
            // 发送消息
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
    public static void SendKafka(String env, ProducerRecord record, Callback callback) {
        String brokers = GetBrokers(env);
        Properties kafkaProps = InitKafkaParams(brokers);

        // 实例化出producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        try {
            // 发送消息
            producer.send(record,  callback);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
    private static String GetBrokers(String env){
        String brokers = "";
        if (env.equals("prod")) {
            brokers = ConfigurationManager.getProperties(Constants.KAFKA_BROKERS_PROD);
        } else if (env.equals("dev")) {
            brokers = ConfigurationManager.getProperties(Constants.KAFKA_BROKERS);
        } else {
            logger.error("获取环境出现错误值: " + env);
            System.exit(0);
        }
        return brokers;
    }

    // 初始化Kafka配置参数
    private static Properties InitKafkaParams(String brokers){
        // 配置Kafka Producer
        Properties kafkaProps = new Properties();
        String key_serializer = ConfigurationManager.getProperties(Constants.KAFKA_KEY_SERIALIZER);
        String value_serializer = ConfigurationManager.getProperties(Constants.KAFKA_VALUE_SERIALIZER);
        // Kafka通用配置
        kafkaProps.put("bootstrap.servers", brokers);
        kafkaProps.put("key.serializer", key_serializer);
        kafkaProps.put("value.serializer", value_serializer);
        return kafkaProps ;
    }
}