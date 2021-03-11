package com.realtime.utils.flink

import com.alibaba.fastjson.JSONObject
import com.realtime.conf.Constants
import com.realtime.utils.phoenix.DimUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util


// 通过Phoenix向Hbase表中写数据
class DimSink extends RichSinkFunction[JSONObject]{

  var connection: Connection = null

  @throws[Exception]
  override def open(parameters: Configuration) = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    connection= DriverManager.getConnection(Constants.PHOENIX_PROD)
  }


  // 生成语句提交hbase
  override def invoke(jsonObject: JSONObject, context: SinkFunction.Context) = {
    val tableName: String = jsonObject.getString("sink_table")
    val dataJsonObj: JSONObject = jsonObject.getJSONObject("data")
    if (dataJsonObj != null && dataJsonObj.size() > 0) {
      val upsertSql: String = genUpsertSql(tableName.toUpperCase, jsonObject.getJSONObject("data"))
      try {
        println(upsertSql)
        val ps: PreparedStatement = connection.prepareStatement(upsertSql)
        ps.executeUpdate()
        connection.commit()
        ps.close()
      } catch {
        case e: Exception => {
          e.printStackTrace()
          throw new RuntimeException("执行sql失败！！！")
        }
      }
    }

    // 如果维度数据发生变化，那么清空当前数据在Redis中的缓存
    if(jsonObject.getString("type").equals("update") ||jsonObject.getString("type").equals("delete")){
      DimUtil.deleteCached(tableName,dataJsonObj.getString("id"))
    }
  }

  def genUpsertSql(tableName: String, jsonObject: JSONObject): String = {
    val fields: util.Set[String] = jsonObject.keySet()
    val upsertSql: String = "upsert into "+Constants.PHOENIX_PROD+"."+tableName+"("+StringUtils.join(fields,",")+")"
    val valuesSql: String = " values ('" + StringUtils.join(jsonObject.values, "','") + "')"
    upsertSql + valuesSql
  }
}
