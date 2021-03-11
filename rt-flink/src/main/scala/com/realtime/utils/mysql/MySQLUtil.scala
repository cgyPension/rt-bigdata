package com.realtime.utils.mysql

import com.google.common.base.CaseFormat
import org.apache.commons.beanutils.BeanUtils
import com.realtime.bean.{TableProcess, TableProcess2}

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util
import scala.collection.JavaConversions._
// 从MySQL工具类
object MySQLUtil {

  // mysql查询方法，根据给定的class类型 返回对应类型的元素列表  是否开启下划线转驼峰的映射
  def queryList[T](sql: String, clazz: Class[T], underScoreToCamel: Boolean) = {

    var ps: PreparedStatement = null
    var rs: ResultSet = null
    var conn: Connection = null

    try {
      // 创建数据库操作对象
      conn = SqlServerConnection.getConnection("prod")
      // 创建数据库操作对象
      ps = conn.prepareStatement(sql)
      // 执行SQL语句
      rs = ps.executeQuery()

      // 处理结果集
      val md: ResultSetMetaData = rs.getMetaData
      // 声明集合对象，用于封装返回结果
      val resultList: util.ArrayList[T] = new util.ArrayList[T]()
      // 每循环一次，获取一条查询结果
      while (rs.next()) {
        // 通过反射创建要将查询结果转换为目标类型的对象
        val obj: T = clazz.newInstance()
        // 对查询出的列进行遍历，每遍历一次得到一个列名
        var i: Int = 1
        while (i <= md.getColumnCount) {
          var propertyName: String = md.getColumnName(i)
          // 如果开启了下划线转驼峰的映射，那么将列名里的下划线转换为属性的打印
          if (underScoreToCamel) {
            // 直接调用Google的guava的CaseFormat  LOWER_UNDERSCORE小写开头+下划线->LOWER_CAMEL小写开头+驼峰
            propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, propertyName)
          }
          // 调用apache的commons-bean中的工具类，给Bean的属性赋值
          BeanUtils.setProperty(obj, propertyName, rs.getObject(i))
          i += 1
        }
        resultList.add(obj)
      }
      resultList
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new RuntimeException("查询mysql失败！")
      }
    } finally {
      // 释放资源
      SqlServerConnection.closeResource(rs,ps,conn)
    }
  }

  // 验证测试
  def main(args: Array[String]): Unit = {
    val tableProcesses: util.ArrayList[TableProcess2.type] = queryList("select * from table_process", classOf[TableProcess.type], true)
    // val tableProcesses: util.ArrayList[TableProcess.type] = queryList("select * from table_process", classOf[TableProcess.type], true)

    for (tableProcess <- tableProcesses) {
      println(tableProcess)
    }
  }
}
