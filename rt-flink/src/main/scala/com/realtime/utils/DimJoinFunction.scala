package com.realtime.utils

import com.alibaba.fastjson.JSONObject

// 维度关联查询的接口
trait DimJoinFunction[T] {
  /**
   * 需要实现如何把结果装配给数据流对象
   * @param t  数据流对象
   * @param jsonObject   异步查询结果
   * @throws Exception
   */
  @throws[Exception]
  def join( t: T, jsonObject: JSONObject): Unit

  /**
   * 需要实现如何从流中对象获取主键
   * @param t  数据流对象
   */
  def getKey(t: T): String
}
