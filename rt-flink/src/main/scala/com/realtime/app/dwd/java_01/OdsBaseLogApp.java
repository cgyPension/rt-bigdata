package com.realtime.app.dwd.java_01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtime.utils.kafka.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Author: Felix
 * Desc: 从Kafka中读取ods层用户行为日志数据
 */
public class OdsBaseLogApp {
    //定义用户行为主题信息
    private static final String TOPIC_START ="dwd_start_log";
    private static final String TOPIC_PAGE ="dwd_page_log";
    private static final String TOPIC_DISPLAY ="dwd_display_log";

    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        //创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度  这里和kafka分区数保持一致
        env.setParallelism(4);
        //设置CK相关的参数
        //设置精准一次性保证（默认）  每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //指定消费者配置信息
        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        //TODO 1.从kafka中读取数据
        //调用Kafka工具类，从指定Kafka主题读取数据
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );
        //打印测试
        jsonObjectDS.print();
        //执行
        env.execute("dwd_base_log Job");
    }
}
