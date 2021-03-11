package com.realtime.utils

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

// 线程池工具类
/*
获取单例的线程池对象
corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue任务队列中去；
maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量；
keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
unit:keepAliveTime的单位
workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
*/
object ThreadPoolUtil {

  var pool: ThreadPoolExecutor = null

  def getInstance: ThreadPoolExecutor = {
    if (pool == null) {
       classOf[ThreadPoolUtil.type] synchronized{
        if (pool == null) {
         println ("开辟程池！！！！！");
          pool=new ThreadPoolExecutor(4, 20, 300, TimeUnit.SECONDS, new LinkedBlockingDeque[Runnable](Integer.MAX_VALUE))
        }
      }
    }
    pool
  }


}
