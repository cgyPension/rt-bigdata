package com.realtime.utils

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date
/**
 * Desc:日期转换工具类
JDK8的DateTimeFormatter替换SimpleDateFormat，因为SimpleDateFormat存在线程安全问题
 */
object DateTimeUtil {

  val formator: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   * 将Date日期转换为字符串
   * @return
   */
  def toYMDhms(date: Date): String = {
    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, ZoneId.systemDefault)
    formator.format(localDateTime)
  }

  /**
   * 将字符串日期转换为时间毫秒数
   * @param dateStr
   * @return
   */
  def toTs(YmDHms: String): Long = {
    //    System.out.println ("YmDHms:"+YmDHms);
    val localDateTime: LocalDateTime = LocalDateTime.parse(YmDHms, formator)
    val ts: Long = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli
    ts
  }
}
