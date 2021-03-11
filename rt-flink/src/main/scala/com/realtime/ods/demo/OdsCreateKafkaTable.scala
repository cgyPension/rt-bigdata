package com.realtime.ods.demo

object OdsCreateKafkaTable {

  def user_test =
    """
      |CREATE TABLE user_test (
      |user_id BIGINT,
      |item_id BIGINT,
      |category_id BIGINT,
      |behavior STRING,
      |`record_time` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp',
      |ts TIMESTAMP(3) ) WITH (
      |'connector' = 'kafka',
      |'topic' = 'user_test',
      |'properties.group.id' = 'local-sql-test',
      |'scan.startup.mode' = 'latest-offset',
      |'properties.bootstrap.servers' = 'nlocalhost:9200',
      |'format' = 'json' ,
      |'json.timestamp-format.standard' = 'SQL',
      |'scan.topic-partition-discovery.interval'='10000',
      |'json.fail-on-missing-field' = 'false',
      |'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin


  def ods_fact_user_ippv =
    """
      |CREATE TABLE ods_fact_user_ippv (
      |    user_id      STRING,
      |    client_ip    STRING,
      |    client_info  STRING,
      |    pagecode     STRING,
      |    access_time  TIMESTAMP,
      |    dt           STRING,
      |    WATERMARK FOR access_time AS access_time - INTERVAL '5' SECOND  -- 定义watermark
      |) WITH (
      |   'connector' = 'kafka',
      |    'topic' = 'user_ippv',
      |    'scan.startup.mode' = 'earliest-offset',
      |    'properties.group.id' = 'group1',
      |    'properties.bootstrap.servers' = 'xxx:9092',
      |    'format' = 'json',
      |    'json.fail-on-missing-field' = 'false',
      |    'json.ignore-parse-errors' = 'true'
      |)
      |""".stripMargin

  def result_total_pvuv_min =
    """
      |CREATE TABLE result_total_pvuv_min (
      |    do_date     STRING,     -- 统计日期
      |    do_min      STRING,      -- 统计分钟
      |    pv          BIGINT,     -- 点击量
      |    uv          BIGINT,     -- 一天内同个访客多次访问仅计算一个UV
      |    currenttime TIMESTAMP,  -- 当前时间
      |    PRIMARY KEY (do_date, do_min) NOT ENFORCED
      |) WITH (
      |  'connector' = 'upsert-kafka',
      |  'topic' = 'result_total_pvuv_min',
      |  'properties.bootstrap.servers' = 'xxx:9092',
      |  'key.json.ignore-parse-errors' = 'true',
      |  'value.json.fail-on-missing-field' = 'false',
      |  'key.format' = 'json',
      |  'value.format' = 'json',
      |  'value.fields-include' = 'ALL'
      |)
      |""".stripMargin


}
