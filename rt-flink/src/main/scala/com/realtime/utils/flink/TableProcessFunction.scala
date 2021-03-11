package com.realtime.utils.flink

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import com.realtime.bean.{TableProcess, TableProcess2}
import com.realtime.conf.Constants
import org.apache.flink.configuration.Configuration

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import scala.collection.immutable.HashMap
import scala.collection.mutable
import java.util.{Map, Timer, TimerTask}
import com.realtime.utils.mysql.MySQLUtil

import scala.collection.JavaConversions._
//import java.util.HashMap

// 用于对业务数据进行分流处理的自定义处理函数
class TableProcessFunction(outputTag: OutputTag[JSONObject]) extends ProcessFunction[JSONObject, JSONObject] {

  // 因为要将维度数据写到侧输出流，所以定义一个侧输出流标签
  var outputTag: OutputTag[JSONObject] = null

  // 用于在内存中存储表配置对象 [表名,表配置信息]
  private val tableProcessMap: Map[String, TableProcess2] = new HashMap[String, TableProcess2]()

  // 表示目前内存中已经存在的HBase表
  private val existsTables: mutable.Set[String] = new mutable.HashSet()

  // 声明Phoenix连接
  private var connection: Connection = null


  @throws[Exception]
  override def open(parameters: Configuration) = {
    // 初始化Phoenix连接
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    connection = DriverManager.getConnection(Constants.PHOENIX_PROD)
    // 初始化配置表信息
    initTableProcessMap()
    // 开启定时任务,用于不断读取配置表信息  从现在起过delay毫秒以后，每隔period更新一次
    val timer: Timer = new Timer()
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        initTableProcessMap()
      }
    }, 5000, 5000)
  }


  // 读取MySQL中配置表信息，存入到内存Map中
  def initTableProcessMap() = {
    println("更新配置的处理信息")
    // 查询MySQL中的配置表数据
    val tableProcessList: util.ArrayList[TableProcess2.type] = MySQLUtil.queryList("select * from table_process", classOf[TableProcess2.type], true)
    // 遍历查询结果,将数据存入结果集合

    for (tableProcess <- tableProcessList) {
      //获取源表表名
      //      val sourceTable: String = tableProcess.getSourceTable
      /*      val sourceTable: String = tableProcess match {
        case TableProcess2(sourceTable,operateType, sinkType,sinkTable, sinkColumns, sinkPk, sinkExtend) =>sourceTable
      }*/
      val sourceTable: String = tableProcess.asInstanceOf[TableProcess2].sourceTable

      //获取数据操作类型
      val operateType: String = tableProcess.asInstanceOf[TableProcess2].operateType

      //获取结果表表名
      val sinkTable: String = tableProcess.asInstanceOf[TableProcess2].sinkTable

      //获取sink类型
      val sinkType: String = tableProcess.asInstanceOf[TableProcess2].sinkType

      val sinkColumns: String = tableProcess.asInstanceOf[TableProcess2].sinkColumns
      var sinkPk: String = tableProcess.asInstanceOf[TableProcess2].sinkPk
      val sinkExtend: String = tableProcess.asInstanceOf[TableProcess2].sinkExtend

      //拼接字段创建主键
      val key: String = sourceTable + ":" + operateType
      //将数据存入结果集合
      tableProcessMap.put(key, tableProcess.asInstanceOf[TableProcess2])
      //如果是向Hbase中保存的表，那么判断在内存中维护的
      if ("insert".equals(operateType) && "hbase".equals(sinkType)) {
        val notExist: Boolean = existsTables.add(sourceTable)
        // 如果表信息数据不存在内存,则在Phoenix中创建新的表
        if (notExist) {
          checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend)
        }
      }
    }
    if (tableProcessMap == null || tableProcessMap.size() == 0) {
      throw new RuntimeException("缺少处理信息")
    }
  }

  // 如果MySQL的配置表中添加了数据，该方法用于检查Hbase中是否创建过表，如果没有则通过Phoenix中创建新增的表
  def checkTable(tableName: String, sinkColumns: String, sinkPk: String, sinkExtend: String) = {
    // 主键不存在，则给定默认值
    if (sinkPk == null) sinkPk = "id"

    // 扩展字段不存在，则给定默认值
    if (sinkExtend == null) sinkExtend = ""

    // 创建字符串拼接对象用于拼接 建表sql
    val createSql: StringBuilder = new StringBuilder("create table if not exists " + Constants.HBASE_SCHEMA + "." + tableName + "(")

    //将列做切分,并拼接至建表语句SQL中
    val columns: Array[String] = sinkColumns.split(",")
    for (column <- columns) {
      if (sinkPk.equals(column)) {
        createSql.append(column).append("varchar")
        createSql.append("primary key")
      } else {
        createSql.append("info.").append(column).append("varchar")
      }
      createSql.append(",")
    }
    createSql.append(")")
    createSql.append(sinkExtend)

    try {
      println(createSql)
      val ps: PreparedStatement = connection.prepareStatement(createSql.toString())
      ps.execute(this.getClass.getName)
      ps.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw new RuntimeException("建表失败！！！")
      }
    }

  }

  // 校验字段，过滤掉多余的字段
  def filterColumn(data: JSONObject, sinkColumns: String) = {
    val cols: Array[String] = sinkColumns.split(",")
    val entries: util.Set[Map.Entry[String, Object]] = data.entrySet()
    val columnList: List[String] = cols.toList

    val iter: util.Iterator[Map.Entry[String, Object]] = entries.iterator()

    while (iter.hasNext) {
      val entry: Map.Entry[String, Object] = iter.next()
      if (!columnList.contains(entry.getKey())) iter.remove
    }
  }

  // 核心处理方法，根据MySQL配置表的信息为每条数据打标签，走kafka还是hbase
  @throws[Exception]
  override def processElement(jsonObj: JSONObject, ctx: ProcessFunction[JSONObject, JSONObject]#Context, out: Collector[JSONObject]) = {
    val table: String = jsonObj.getString("tabel")
    var type1: String = jsonObj.getString("type")
    val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")

    // 如果是使用Maxwell的初始化功能，那么type类型为bootstrap-insert,我们这里也标记为insert，方便后续处理
    if (type1.equals("bootstrap-insert")) {
      type1 = "insert"
      jsonObj.put("type", type1)
    }

    // 获取配置表的信息
    if (tableProcessMap != null && tableProcessMap.size() > 0) {
      val key: String = table + ":" + type1
      val tableProcess: TableProcess2 = tableProcessMap.get(key)
      if (tableProcess != null) {
        jsonObj.put("sink_table", tableProcess.asInstanceOf[TableProcess2].sinkTable)
        val sinkColumns: String = tableProcess.asInstanceOf[TableProcess2].sinkColumns
        if (sinkColumns != null && sinkColumns.length > 0) {
          filterColumn(jsonObj.getJSONObject("data"), sinkColumns)
        }
      } else {
        println("没有这个key：" + key)
      }
      if (tableProcess != null && tableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.asInstanceOf[TableProcess2].sinkType)) {
        ctx.output(outputTag, jsonObj)
      } else if (tableProcess != null && tableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.asInstanceOf[TableProcess2].sinkType)) {
        out.collect(jsonObj)
      }
    }

  }


}



