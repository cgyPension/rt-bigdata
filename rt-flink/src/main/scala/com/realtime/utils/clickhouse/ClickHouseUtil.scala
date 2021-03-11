package com.realtime.utils.clickhouse

import com.realtime.bean.TransientSink
import com.realtime.conf.Constants
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.JdbcExecutionOptions
import org.apache.flink.connector.jdbc.JdbcSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.lang.reflect.Field
import scala.util.control.Breaks._

// 操作ClickHouse的工具类
/**
 * 其中flink-connector-jdbc 是官方通用的jdbcSink包。只要引入对应的jdbc驱动，flink可以用它应对各种支持jdbc的数据库，比如phoenix也可以用它。
        但是这个jdbc-sink只支持数据流对应一张数据表。如果是一流对多表，就必须通过自定义的方式实现了，比如之前的维度数据。
        虽然这种jdbc-sink只能一流对一表，但是由于内部使用了预编译器，所以可以实现批量提交以优化写入速度。
 */
object ClickHouseUtil {
  // 获取针对ClickHouse的JdbcSink
  def getJdbcSink[T](sql: String)= {
    val sink: SinkFunction[T] = JdbcSink.sink[T](sql, (jdbcPreparedStatement, t) => {
      val fields: Array[Field] = t.getClass().getDeclaredFields
      var skipOffset: Int = 0
      for (i <- 0 until fields.length) {
        val field: Field = fields(i)
        //通过反射获得字段上的注解
        val transientSink: TransientSink = field.getAnnotation(classOf[TransientSink])
        breakable(
          if (transientSink != null) {
            // 如果存在注解
            println("跳过字段：" + field.getName)
            skipOffset += 1
            break()
          })
        field.setAccessible(true)
        try {
          val o: Object = field.get(t)
          //i代表流对象字段的下标，
          // 公式：写入表字段位置下标 = 对象流对象字段下标 + 1 - 跳过字段的偏移量
          // 一旦跳过一个字段 那么写入字段下标就会和原本字段下标存在偏差
          jdbcPreparedStatement.setObject(i + 1 - skipOffset, o)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }

      }
    }, new JdbcExecutionOptions.Builder().withBatchSize(2).build,
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(Constants.JDBC_CLICKHOUSE_URL_PROD)
        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build)
    sink
  }


}
