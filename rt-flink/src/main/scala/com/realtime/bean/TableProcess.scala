package com.realtime.bean

import lombok.Data

// 创建配置表实体类

@Data
object TableProcess {
  // 动态分流Sink常量
  val SINK_TYPE_HBASE: String = "HBASE"
  val SINK_TYPE_KAFKA: String = "KAFKA"
  val SINK_TYPE_CK: String = "CLICKHOUSE"

  //来源表
  val sourceTable: String = null
  //操作类型 insert,update,delete
  val operateType: String = null
  //输出类型 hbase kafka
  val sinkType: String = null
  //输出表(主题)
  val sinkTable: String = null
  //输出字段
  val sinkColumns: String = null
  //主键字段
  val sinkPk: String = null
  //建表扩展
  val sinkExtend: String = null

}
