package com.realtime.conf;

//import org.nustaq.serialization.util.test;

/**
 * JDBC常量接口
 *
 */

public interface Constants {
	/**
	 * 项目配置常量
	 */
	public String JDBC_DRIVER="jdbc.driver";
	public String JDBC_DATASOURCE_SIZE="jdbc.datasource.size";
	public String JDBC_URL="jdbc.url";
	public String JDBC_USER="jdbc.user";
	public String JDBC_PASSWORD="jdbc.password";
	public String JDBC_URL_PROD="jdbc.url.prod";
	public String JDBC_USER_PROD="jdbc.user.prod";
	public String JDBC_PASSWORD_PROD="jdbc.password.prod";


	public String JDBC_MSSQL_DRIVER="jdbc.mssql.driver";
	public String JDBC_MSSQL_URL="jdbc.mssql.url";
	public String JDBC_MSSQL_USER="jdbc.mssql.user";
	public String JDBC_MSSQL_PASSWORD="jdbc.mssql.password";
	public String JDBC_MSSQL_URL_PROD="jdbc.mssql.url.prod";
	public String JDBC_MSSQL_USER_PROD="jdbc.mssql.user.prod";
	public String JDBC_MSSQL_PASSWORD_PROD="jdbc.mssql.password.prod";

	// Kafka 配置
	public String KAFKA_MAX_PARTITION_FETCH_SIZE="max.partition.fetch.bytes"; //Message消息体分配的内存大小
	public String KAFKA_RECEIVE_BUFFER_SIZE="receive.buffer.bytes"; //Message消息体分配的内存大小
	public String KAFKA_MAX_POLL_SIEZ="max.poll.records"; // 单词最大拉取

	public String KAFKA_KEY_DESERIALIZER="kafka.key.deserializer"; // Kafka: key反序列化
	public String KAFKA_VALUE_DESERIALIZER="kafka.value.deserializer"; //Kafaka: value反序列化
	public String KAFKA_KEY_SERIALIZER="kafka.key.serializer"; // Kafka: key反序列化
	public String KAFKA_VALUE_SERIALIZER="kafka.value.serializer"; //Kafaka: value反序列化

	public String KAFKA_BROKERS="kafka.brokers";
	public String KAFKA_BROKERS_PROD="kafka.brokers.prod";

	// Spark 配置
	public String SPARK_MASTER="spark.master"; // SPARK: 设置Master，运行模式
	public String SPARK_MASTER_PROD="spark.master_prod"; // SPARK: 设置Master，运行模式

	// Zookeeper 配置
	public String ZK_SERVER_URLS="zk.server.urls"; // Zookeeper: 生产zk地址
	public String ZK_SERVER_URLS_PROD="zk.server.urls.prod"; // Zookeeper: 生产zk地址
	public String ZK_SERVER_MAX_ACTIVATE="zk.server.max.active";
	public String ZK_SERVER_WAIT_MAX="zk.server.wait.max";
	public String ZK_SERVER_SESSION_TIMEOUT="zk.server.session.timeout";

	// Redis 连接池
	public String REDIS_SERVER_HOST="redis.server.host";
	public String REDIS_SERVER_PORT="redis.server.port";
	public String REDIS_SERVER_PASSWORD="redis.server.password";
	public String REDIS_SERVER_HOST_PROD="redis.server.host.prod";
	public String REDIS_SERVER_PORT_PROD="redis.server.port.prod";
	public String REDIS_SERVER_PASSWORD_PROD="redis.server.password.prod";
    // Redis 哨兵模式
	public String REDIS_SENTINEL_MASTER_NAME="redis.sentinel.master.name";
	public String REDIS_SENTINEL_HOSTS="reids.sentinel.hosts";
	public String REDIS_SENTINEL_HOSTS_PROD="reids.sentinel.hosts.prod";

	// Clickhouse
	public String JDBC_CLICKHOUSE_URL="jdbc.clickhouse.url";
	public String JDBC_CLICKHOUSE_USER="jdbc.clickhouse.user";
	public String JDBC_CLICKHOUSE_PASSWORD="jdbc.clickhouse.password";
	public String JDBC_CLICKHOUSE_URL_PROD="jdbc.clickhouse.url.prod";
	public String JDBC_CLICKHOUSE_USER_PROD="jdbc.clickhouse.user.prod";
	public String JDBC_CLICKHOUSE_PASSWORD_PROD="jdbc.clickhouse.password.prod";

	// Phoenix
	public String PHOENIX="phoenix.brokers";
	public String PHOENIX_PROD="phoenix.brokers.prod";

	public static final String HBASE_SCHEMA="GMALL2021_REALTIME";
}