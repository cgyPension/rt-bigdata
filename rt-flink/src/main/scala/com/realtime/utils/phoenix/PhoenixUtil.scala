package com.realtime.utils.phoenix

import com.alibaba.fastjson.JSONObject
import com.realtime.conf.Constants

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.beanutils.BeanUtils


import java.util
import org.apache.commons.beanutils.BeanUtils

import scala.collection.JavaConversions._
// 查询Phoenix的工具类
object PhoenixUtil {
  var conn: Connection = null

  def main(args: Array[String]): Unit = {
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(Constants.PHOENIX_PROD)
      conn.setSchema(Constants.HBASE_SCHEMA)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val objectList: util.List[JSONObject] = queryList("select * from  base_trademark", classOf[JSONObject])
    println(objectList)
  }


  def queryInit(): Unit = {
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      conn = DriverManager.getConnection(Constants.PHOENIX_PROD)
      conn.setSchema(Constants.HBASE_SCHEMA)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  def queryList[T](sql: String, clazz: Class[T]): util.List[T] = {
    if (conn == null) queryInit

    val resultList: util.List[T] = new util.ArrayList()
    var ps: PreparedStatement = null

    try {
      ps = conn.prepareStatement(sql)
      val rs: ResultSet = ps.executeQuery
      val md: ResultSetMetaData = rs.getMetaData
      while (rs.next()) {
        val rowData: T = clazz.newInstance()
        for (i <- 0 until md.getColumnCount) {
          BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i))
        }
        resultList.add(rowData)
      }
      ps.close()
      rs.close()
    }catch
      {
        case e: Exception =>
          e.printStackTrace()
      }
    resultList
  }



}
