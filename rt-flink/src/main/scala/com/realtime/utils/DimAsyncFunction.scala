package com.realtime.utils

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import com.alibaba.fastjson.JSONObject
import com.realtime.utils.phoenix.DimUtil
import java.util.concurrent.{ExecutorService, ThreadPoolExecutor}
import java.util

/**
 * Desc: 自定义维度查询异步执行函数
 * RichAsyncFunction：   里面的方法负责异步查询
 * DimJoinFunction：     里面的方法负责将维表和主流进行关联
 * DimAsyncFunction 核心的类是AsyncDataStream，这个类有两个方法一个是有序等待（orderedWait），一个是无序等待（unorderedWait）
 */

abstract class DimAsyncFunction[T] extends RichAsyncFunction[T, T] with DimJoinFunction[T] {

  var executorService: ExecutorService = null

  //维度的表名
  var tableName:String = null

  def this(tableName: String) {
    this()
    this.tableName = tableName
  }

  override def open(parameters: Configuration) = {
    println("获得线程池！")
    executorService = ThreadPoolUtil.getInstance
  }

  override def asyncInvoke(obj: T, resultFuture: ResultFuture[T]) = {
    executorService.submit(new Runnable {
      override def run() = {
        try {
          val start: Long = System.currentTimeMillis
          // 从流对象中获取主键
          val key: String = getKey(obj)
          // 根据主键获取维度对象数据
          val dimJsonObject: JSONObject = DimUtil.getDimInfo(tableName, key)
          println("dimJsonObject:" + dimJsonObject)
          if (dimJsonObject != null) { //维度数据和流数据关联
            join(obj, dimJsonObject)
          }
          println("obj:" + obj)
          val end: Long = System.currentTimeMillis
          println("异步耗时：" + (end - start) + "毫秒")
          resultFuture.complete(obj.asInstanceOf[Iterable[T]]) // 要把obj转成迭代类型
        } catch {
          case e: Exception => {
            e.printStackTrace()
            throw new RuntimeException(tableName + "维度异步查询失败")
          }
        }
      }
    })
  }


}
