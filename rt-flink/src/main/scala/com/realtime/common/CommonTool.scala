package com.realtime.common

import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

/**
 *@Description: 非组件非业务规则的公共方法
 */
object CommonTool {

  // TODO ============================================ udf函数 ============================================
  // ============================================ 字符串相关 ============================================
  // 过滤表情
  def getSubsidy(str: String): String = {
    var out = new StringBuffer()
    if (str == null || ("".equals(str))){
      ""
    }else{
      var chars = str.toCharArray()
      for(i <- 0 until chars.length) {
        if(
          (chars(i) >= 19968 && chars(i) <= 40869) //中日朝兼容形式的unicode编码范围： U+4E00——U+9FA5
            || (chars(i) >= 11904 && chars(i) <= 42191)//中日朝兼容形式扩展
            || (chars(i) >= 63744 && chars(i) <= 64255)//中日朝兼容形式扩展
            || (chars(i) >= 65072 && chars(i) <= 65103)//中日朝兼容形式扩展
            || (chars(i) >= 65280 && chars(i) <= 65519)//全角ASCII、全角中英文标点、半宽片假名、半宽平假名、半宽韩文字母的unicode编码范围：U+FF00——U+FFEF
            || (chars(i) >= 32 && chars(i) <= 126)//半角字符的unicode编码范围：U+0020-U+007e
            || (chars(i) >= 12289 && chars(i) <= 12319)//全角字符的unicode编码范围：U+3000——U+301F
        ) {
          out.append(chars(i))
        }
      }
      out.toString
    }
  }

  // 删除表情
  def removeEmoji(str: String): String = {
    if (StringUtils.isBlank(str)) {
      ""
    } else {
      val characterFilter = "[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]"
      str.replaceAll(characterFilter, "")
    }
  }

  // 特殊字符替换 表情
  def specialStr(str: String): String = {
    var newStr: String = ""
    if (StringUtils.isNotBlank(str)) {
      newStr = str.replaceAll("[\ud800\udc00-\udbff\udfff\ud800-\udfff]", "")
    }
    newStr.trim
  }






  // TODO ============================================ 代码方法 能用sql实现 ============================================
  // ============================================ 时间相关 ============================================
  // 得到每隔10分钟的时间段
  // 2021-01-21 14:58:22 →→→ 14:50:00-14:59:59
  def getTimeInterval(str: String) = {
    val time = str.substring(11, 19)
    val preTime = time.substring(0, 3) + time.substring(3, 4) + "0" + ":00"
    val hh = time.substring(0, 3)
    val mm = time.substring(3, 4)
    val aftTime = hh + mm + "9:59"
    preTime + "-" + aftTime
  }

  /**
   * 获取当天前后的日期 2020-03-26
   * @param strDate
   * @param day
   * Calendar是日历类，该类将所有可能用到的时间信息封装为静态成员变量，方便获取
   */
  def getDaysBefore(strDate: String, day: Int): String = {
    var before: String = "未知"
    if (StringUtils.isNotBlank(strDate)) {
      val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
      val cal: Calendar = Calendar.getInstance()
      val dt = formatter.parse(strDate)
      cal.setTime(dt)
      // 修改当前时间为n天后/前
      cal.add(Calendar.DATE, day)
      before = formatter.format(cal.getTime())
    }
    before
  }

  // 获取两个日期相差秒数
  def getDateBetweenTime(startDateStr: String, lastDateStr: String): Long = {
    var between_days: Long = 0
    if (StringUtils.isNotBlank(startDateStr) && StringUtils.isNotBlank(lastDateStr)) {
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
      val cal = Calendar.getInstance()
      cal.setTime(sdf.parse(startDateStr))
      val time1 = cal.getTimeInMillis()
      cal.setTime(sdf.parse(lastDateStr))
      val time2 = cal.getTimeInMillis()
      between_days = Math.abs(time2 - time1) / 1000
    }
    between_days
  }

  // 获取当月最后一天
  def getMonthLastday(date: String): String = {
    var monthLastday: String = null
    if (StringUtils.isNotBlank(date)) {
      val year = date.substring(0, 4).toInt
      // 月
      val month = date.substring(5, 7).toInt
      // 日
      val day = date.substring(8, 10).toInt
      val cal = Calendar.getInstance
      // 设置年
      cal.set(Calendar.YEAR, year)
      // 设置月  注意: 需要 -1，才是原来月份的值
      cal.set(Calendar.MONTH, month - 1)
      // 设置最后一天
      cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DATE));

      monthLastday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime())
    }

    monthLastday
  }

  // 获取星期几 返回string
  def getDayWeek(dateStr: String) = {
    var dayWeek: String = null
    if (StringUtils.isNotBlank(dateStr)) {
      val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
      val cal: Calendar = Calendar.getInstance()
      val dt = formatter.parse(dateStr)
      var weekDays = Array("星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六")
      cal.setTime(dt)
      var w = cal.get(Calendar.DAY_OF_WEEK) - 1
      if (w < 0) {
        w = 0
      }
      dayWeek = weekDays(w)
    }
    dayWeek
  }

  // TODO ======================================== spark 先全部注释掉========================================
  /**
   * 防止spark每分钟刷写数据到数仓中小文件过多的问题
   *
   * 解决办法：
   *   1. 获取目标表对应分区的所有数据
   *   2. 当前接收到的数据与查询到的数据进行合并
   *   3. 将合并后的数据覆盖到临时表对应的分区中
   *   4. 查询临时表对应分区的数据，将数据覆盖到目标表中对应的分区中
   *
   * @param spark           sparkSession
   * @param targetTableName 目标表
   * @param tempTableName   临时表
   * @param PartitionNo     分区号
   * @param newDF           当前流数据
   */
  /*def saveDataToTempAndSaveDataToTarget(spark: SparkSession, targetTableName: String, tempTableName: String, partitionName: String, PartitionNo: String, newDF: DataFrame): Unit = {
    // TODO 获取目标表对应分区的所有的数据
    val targetDF = spark.sql(
      s"""
         |select * from $targetTableName
         |""".stripMargin).where(s" $partitionName = '$PartitionNo'")

    // TODO 目标表所有的数据跟当前的数据进行合并
    val allDF = newDF.union(targetDF)

    // TODO 将当前的数据全部刷写到临时表中
    allDF.repartition(6).write.mode(SaveMode.Overwrite).insertInto(tempTableName)

    // TODO 刷新临时表的元数据（不然后面有可能会报错）
    spark.catalog.refreshTable(tempTableName)

    // TODO 查询历史表的所有数据，直接刷写覆盖目标表对应的分区
    val tempDF = spark.sql(
      s"""
         |select * from $tempTableName

         |""".stripMargin).where(s" $partitionName = '$PartitionNo'")

    tempDF.repartition(6).write.mode(SaveMode.Overwrite).insertInto(targetTableName)
  }*/

 /* def saveDataToTempAndSaveDataToTarget_new(spark: SparkSession, targetTableName: String, partitionName: String, PartitionNo: String, newDF: DataFrame, numPartitions: Int): Unit = {
    // TODO 获取目标表对应分区的所有的数据
    val targetDF = spark.sql(
      s"""
         |select * from $targetTableName
         |""".stripMargin).where(s" $partitionName = '$PartitionNo'")
    // TODO 目标表所有的数据跟当前的数据进行合并
    val allDF = newDF.union(targetDF).localCheckpoint()
    // TODO 将当前的数据全部刷写到临时表中
    allDF.repartition(numPartitions).write.mode(SaveMode.Overwrite).insertInto(targetTableName)
  }


  // 按天删除mysql,按时间范围按天删除, 传开始和结束时间, 删除条件字段, 表名 正常删会慢
  def deleteByDay(spark: SparkSession, config: String, tableName:String, startDate:String, endDate:String, field:String) = {
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("mysqlconfig.properties"))
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val startSecond = sdf.parse(startDate).getTime
    val endSecond = sdf.parse(endDate).getTime
    for (timestamp <- startSecond to endSecond by 86400000) {
      val date = sdf.format(timestamp)
      val delSql = s"delete from $tableName where $field = '$date'"
      val mysqlConfig = spark.sparkContext.getConf
        .set("mysql.url", prop.getProperty(config + "_url"))
        .set("mysql.user", prop.getProperty(config + "_user"))
        .set("mysql.password", prop.getProperty(config + "_pwd"))
        .set("mysql.sql", delSql)
      MySqlWriter.execute(mysqlConfig)
    }
  }

   // ============================================= 将数据插入到hive中 =============================================
  /**
   * DataFrame 动态将数据插入到不同的分区中
   * @param spark
   * @param df
   * @param saveMode
   */
  def saveDF2HiveWithPartition(spark: SparkSession, df: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Overwrite, partitionNameNameList: List[String]): Unit = {
    if (spark.catalog.tableExists(tableName)) {
      // insertInto方法跟你的参数的顺序有关
      df.repartition(30).write.mode(saveMode).insertInto(tableName)
    } else {
      if (partitionNameNameList.size == 1) {
        df.repartition(30).write.mode(saveMode).partitionBy(partitionNameNameList(0)).saveAsTable(tableName)
      } else if (partitionNameNameList.size == 2) {
        df.repartition(30).write.mode(saveMode).partitionBy(partitionNameNameList(0), partitionNameNameList(1)).saveAsTable(tableName)
      }
    }
  }

  /**
   * DataFrame 将数据插入到不同hive中
   * @param spark
   * @param df
   * @param tableName
   * @param saveMode
   */
  def saveDF2HiveWithNoPartition(spark: SparkSession, df: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    if (spark.catalog.tableExists(tableName)) {
      df.repartition(1).write.mode(saveMode).insertInto(tableName)
    } else {
      df.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    }
  }
  */

}
