package com.realtime.utils.phoenix

import com.alibaba.fastjson.{JSON, JSONObject}
import com.realtime.utils.phoenix.PhoenixUtil
import redis.clients.jedis.Jedis
import com.realtime.utils.reids.RedisUtil

import java.util

// 封装查询维度的工具类DimUtil（直接查询Phoenix） 没有缓存
object DimUtil {

  def getDimInfoNoCache(tableName: String, colNameAndValue: Tuple2[String,String]*) = {
    //组合查询条件
    var wheresql: String = new String(" where ")
    for (i <- 0 until colNameAndValue.length) { //获取查询列名以及对应的值
      val nameValueTuple: Tuple2[String,String] = colNameAndValue(i)
      val fieldName: String = nameValueTuple._1
      val fieldValue: String = nameValueTuple._2
      if (i > 0) wheresql += " and "
      wheresql += fieldName + "='" + fieldValue + "'"
    }
    //组合查询SQL
    val sql: String = "select * from " + tableName + wheresql
    println("查询维度SQL:" + sql)
    var dimInfoJsonObj: JSONObject = null
    val dimList: util.List[JSONObject] = PhoenixUtil.queryList(sql, classOf[JSONObject])
    if (dimList != null && dimList.size > 0) { //因为关联维度，肯定都是根据key关联得到一条记录
      dimInfoJsonObj = dimList.get(0)
    } else {
      println("维度数据未找到:" + sql)
    }
    dimInfoJsonObj
  }

  // 先从Redis中查，如果缓存中没有再通过Phoenix查询 固定id进行关联
  def getDimInfo(tableName: String, id: String): JSONObject = {
    val kv: Tuple2[String, String] = Tuple2("id", id)
    getDimInfo(tableName, kv._2)
  }

  // 先从Redis中查，如果缓存中没有再通过Phoenix查询 可以使用其它字段灵活关联
  def getDimInfo(tableName: String, colNameAndValue: Tuple2[String,String]*) = {
    //组合查询条件
    var wheresql: String = " where "
    var redisKey: String = ""
    for (i <- 0 until colNameAndValue.length) { //获取查询列名以及对应的值
      val nameValueTuple: Tuple2[String,String] = colNameAndValue(i)
      val fieldName: String = nameValueTuple._1
      val fieldValue: String = nameValueTuple._2
      if (i > 0) {
        wheresql += " and "
        // 根据查询条件组合redis key ，
        redisKey += "_"
      }
      wheresql += fieldName + "='" + fieldValue + "'"
      redisKey += fieldValue
    }

    var jedis: Jedis = null
    var dimJson: String = null
    var dimInfo: JSONObject = null
    val key: String = "dim:" + tableName.toLowerCase + ":" + redisKey


    try { // 从连接池获得连接
      jedis = RedisUtil.getJedis
      // 通过key查询缓存
      dimJson = jedis.get(key)
    } catch {
      case e: Exception =>
        println("缓存异常！")
        e.printStackTrace()
    }

    if (dimJson != null) {
      dimInfo = JSON.parseObject(dimJson)
    }else{
      val sql: String = "select * from " + tableName + wheresql
      println("查询维度sql:" + sql)
      val dimList: util.List[JSONObject] = PhoenixUtil.queryList(sql, classOf[JSONObject])
      if (dimList.size > 0) {
        dimInfo = dimList.get(0)
        if (jedis != null) { //把从数据库中查询的数据同步到缓存
          jedis.setex(key, 3600 * 24, dimInfo.toJSONString)
        }
      } else {
        println("维度数据未找到：" + sql)
      }
    }
    if (jedis != null) {
      jedis.close()
      println("关闭缓存连接 ")
    }
   dimInfo
  }

  // 根据key让Redis中的缓存失效
  def deleteCached(tableName: String, id: String): Unit = {
    val key: String = "dim:" + tableName.toLowerCase + ":" + id
    try {
      val jedis: Jedis = RedisUtil.getJedis
      // 通过key清除缓存
      jedis.del(key)
      jedis.close()
    } catch {
      case e: Exception =>
        println("缓存异常！")
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    val dimInfooNoCache: JSONObject = DimUtil.getDimInfoNoCache("base_trademark", Tuple2("id", "13"))
    println(dimInfooNoCache)
  }
}
