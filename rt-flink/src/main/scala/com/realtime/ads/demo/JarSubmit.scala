package com.realtime.ads.demo

object JarSubmit {
  /*def main(args: Array[String]): Unit = {
    if (args == null || args.length == 0) {
      println("参数不全..")
      System.exit(-1)
    }
    val Array(date, table, config) = (args ++ Array(null, null, null)).slice(0, 3)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.debug.maxToStringFields", "200")
      .config("spark.sql.debug.maxToStringFields", "200")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    // ********************************************* udf函数注册 *********************************************

    val getNull = (str: String) => {
      //###jyj_common.getNull(str)
    }
    spark.sqlContext.udf.register("getNull", getNull)


    //mysql输出表
    val TABLE = table

    // ============================================= 配置表 =============================================


    // ********************************************* 增加了一个字段JDEstatus 获取正常单和取消单 *********************************************
    //###jyj_common.getJYJAllOrderCancalAndNormal_test(spark, date, TABLE, config).createOrReplaceTempView("order_table")

    // ********************************************* 开始处理逻辑 *********************************************

    /**
     *  TODO 需求：吉野家私有化报表中心-门店端报表-堂食外卖TC统计报表
     * 从2020年10月1日开始做
     * 更新周期：每天更新一次报表
     *
     */


    spark.sql(
      s"""
         |select orderid,
         |       orderDate,
         |       provinceid,
         |       provincename,
         |       cityid,
         |       cityname,
         |       brandid,
         |       brandname,
         |       storeid,
         |       storename,
         |       jyj_postype as posType,
         |       jyj_ordertype as orderType,
         |       platformkey,
         |       JDEstatus --'是否取消单 0：否 1：是',
         |from order_table
         |""".stripMargin).createOrReplaceTempView("order_table_filter")


    //增加跨月取消单规则，正常单和跨月取消单一起合计 正常为正数 取消单为负数
    //最后再分组聚合
    val result =
      spark.sql(
        s"""
           |select o.orderDate as statDate,
           |       s.province_id as provinceId,
           |       s.province_name as provinceName,
           |       s.city_id as cityId,
           |       s.city_name as cityName,
           |       s.cdbrand_id as brandId,
           |       s.cdbrand_name as brandName,
           |       s.paasstore_id as storeId,
           |       s.cdstore_name as storeName,
           |       sum(if(o.JDEstatus = 'normal', 1, -1)) as tc,
           |       o.posType as posType,
           |       o.orderType as orderType,
           |       s.cdplatform_key as platformKey,
           |       s.cdplatform_name as platformName,
           |       (case when o.JDEstatus = 'normal' then 0 else 1 end) as whetherCancelOrder,
           |       s.cdstore_id as uniteStoreId
           | from order_table_filter o
           | join store_conf s on o.storeid = s.store_id
           | and o.brandId = s.brand_id
           | and o.platformKey = s.platform_key
           | group by
           |o.orderDate,
           |s.province_id,
           |s.province_name,
           |s.city_id,
           |s.city_name,
           |s.cdbrand_id,
           |s.cdbrand_name,
           |s.paasstore_id,
           |s.cdstore_name,
           |o.posType,
           |o.orderType,
           |s.cdplatform_key,
           |s.cdplatform_name,
           |o.JDEstatus,
           |s.cdstore_id
           |order by o.orderDate,s.paasstore_id
           |""".stripMargin)

    //###fp_common.writeMysql(config, result, table, SaveMode.Append)

    spark.close()
  }*/
}
