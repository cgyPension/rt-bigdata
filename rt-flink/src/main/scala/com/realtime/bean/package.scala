package com.realtime

import lombok.Data

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.Target
import java.lang.annotation.ElementType.FIELD
import java.lang.annotation.RetentionPolicy.RUNTIME
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.beans.BeanProperty


package object bean {


  case class TableProcess2(
                            // 动态分流Sink常量
                            SINK_TYPE_HBASE: String = "HBASE",
                            SINK_TYPE_KAFKA: String = "KAFKA",
                            SINK_TYPE_CK: String = "CLICKHOUSE",

                            sourceTable: String, // 来源表
                            operateType: String, // 操作类型 insert,update,delete
                            sinkType: String, // 输出类型 hbase kafka
                            sinkTable: String, // 输出表(主题)
                            sinkColumns: String, // 输出字段
                            var sinkPk: String, // 主键字段
                            sinkExtend: String // 建表扩展
                          )

  // 订单实体类
  case class OrderInfo(
                        id: Long,
                        province_id: Long,
                        order_status: String,
                        user_id: Long,
                        total_amount: BigDecimal,
                        activity_reduce_amount: BigDecimal,
                        coupon_reduce_amount: BigDecimal,
                        original_total_amount: BigDecimal,
                        feight_fee: BigDecimal,
                        expire_time: String,
                        create_time: String,
                        operate_time: String,
                        create_date: String, // 把其他字段处理得到
                        create_hour: String,
                        var create_ts: Long = null
                      ) {
    // JDK8的DateTimeFormatter替换SimpleDateFormat，因为SimpleDateFormat存在线程安全问题
    // create_ts = new SimpleDateFormat("yyyy-MM-dd").parse(create_time).getTime()
    create_ts = new SimpleDateFormat("yyyy-MM-dd").parse(create_time).getTime()
  }

  // 订单明细实体类
  case class OrderDetail(
                          id: Long,
                          order_id: Long,
                          sku_id: Long,
                          order_price: BigDecimal,
                          sku_num: Long,
                          sku_name: String,
                          create_time: String,
                          split_total_amount: BigDecimal,
                          split_activity_amount: BigDecimal,
                          split_coupon_amount: BigDecimal,
                          var create_ts: Long = null
                        ) {
    create_ts = new SimpleDateFormat("yyyy-MM-dd").parse(create_time).getTime()
  }

  // 订单和订单明细关联宽表对应实体类
  case class OrderWide(
                        var detail_id: Long = 0L,
                        var order_id: Long = 0L,
                        var sku_id: Long = 0L,
                        var order_price: BigDecimal = 0.0D,
                        var sku_num: Long = 0L,
                        var sku_name: String = null,
                        var province_id: Long = 0L,
                        var order_status: String = null,
                        var user_id: Long = 0L,

                        var total_amount: BigDecimal = null,
                        var activity_reduce_amount: BigDecimal = null,
                        var coupon_reduce_amount: BigDecimal = null,
                        var original_total_amount: BigDecimal = null,
                        var feight_fee: BigDecimal = null,
                        var split_feight_fee: BigDecimal = null,
                        var split_activity_amount: BigDecimal = null,
                        var split_coupon_amount: BigDecimal = null,
                        var split_total_amount: BigDecimal = null,

                        var expire_time: String = null,
                        var create_time: String = null,
                        var operate_time: String = null,
                        var create_date: String = null, // 把其他字段处理得到
                        var create_hour: String = null,

                        var province_name: String = null, //查询维表得到
                        var province_area_code: String = null,
                        var province_iso_code: String = null,
                        var province_3166_2_code: String = null,

                        var user_age: Int = 0,
                        var user_gender: String = null,

                        var spu_id: Long = 0L, //作为维度数据 要关联进来
                        var tm_id: Long = 0L,
                        var category3_id: Long = 0L,
                        var spu_name: String = null,
                        var tm_name: String = null,
                        var category3_name: String = null
                      ) {
    /**
     * 相关信息并入到 OrderWide 中
     */
    def mergeOrderInfo(orderInfo: OrderInfo): OrderWide = {
      if (orderInfo != null) {
        this.order_id = orderInfo.id
        this.order_status = orderInfo.order_status
        this.create_time = orderInfo.create_time
        this.create_date = orderInfo.create_date
        this.activity_reduce_amount = orderInfo.activity_reduce_amount
        this.coupon_reduce_amount = orderInfo.coupon_reduce_amount
        this.original_total_amount = orderInfo.original_total_amount
        this.feight_fee = orderInfo.feight_fee
        this.total_amount = orderInfo.total_amount
        this.province_id = orderInfo.province_id
        this.user_id = orderInfo.user_id
      }
      this
    }

    def mergeOrderDetail(orderDetail: OrderDetail): OrderWide = {
      if (orderDetail != null) {
        this.detail_id = orderDetail.id
        this.sku_id = orderDetail.sku_id
        this.sku_name = orderDetail.sku_name
        this.order_price = orderDetail.order_price
        this.sku_num = orderDetail.sku_num
        this.split_activity_amount = orderDetail.split_activity_amount
        this.split_coupon_amount = orderDetail.split_coupon_amount
        this.split_total_amount = orderDetail.split_total_amount
      }
      this
    }

    /* def mergeUserInfo(userInfo: UserInfo): OrderWide = {
       if (userInfo != null) {
         this.user_id = userInfo.id

         // 计算用户年龄
         val formatter = new SimpleDateFormat("yyyy-MM-dd")
         val date: Date = formatter.parse(userInfo.birthday)
         val curTs: Long = System.currentTimeMillis()
         val betweenMs: Long = curTs - date.getTime
         val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L

         this.user_age = age.toInt
         this.user_gender = userInfo.gender
         this.user_level = userInfo.user_level
       }
       this
     }*/


    /*    public void mergeOtherOrderWide(OrderWide otherOrderWide){
          this.order_status = ObjectUtils.firstNonNull( this.order_status ,otherOrderWide.order_status);
          this.create_time =  ObjectUtils.firstNonNull(this.create_time,otherOrderWide.create_time);
          this.create_date =  ObjectUtils.firstNonNull(this.create_date,otherOrderWide.create_date);
          this.coupon_reduce_amount =  ObjectUtils.firstNonNull(this.coupon_reduce_amount,otherOrderWide.coupon_reduce_amount);
          this.activity_reduce_amount =  ObjectUtils.firstNonNull(this.activity_reduce_amount,otherOrderWide.activity_reduce_amount);
          this.original_total_amount =  ObjectUtils.firstNonNull(this.original_total_amount,otherOrderWide.original_total_amount);
          this.feight_fee = ObjectUtils.firstNonNull( this.feight_fee,otherOrderWide.feight_fee);
          this.total_amount =  ObjectUtils.firstNonNull( this.total_amount,otherOrderWide.total_amount);
          this.user_id =  ObjectUtils.<Long>firstNonNull(this.user_id,otherOrderWide.user_id);
          this.sku_id = ObjectUtils.firstNonNull( this.sku_id,otherOrderWide.sku_id);
          this.sku_name =  ObjectUtils.firstNonNull(this.sku_name,otherOrderWide.sku_name);
          this.order_price =  ObjectUtils.firstNonNull(this.order_price,otherOrderWide.order_price);
          this.sku_num = ObjectUtils.firstNonNull( this.sku_num,otherOrderWide.sku_num);
          this.split_activity_amount=ObjectUtils.firstNonNull(this.split_activity_amount);
          this.split_coupon_amount=ObjectUtils.firstNonNull(this.split_coupon_amount);
          this.split_total_amount=ObjectUtils.firstNonNull(this.split_total_amount);
        }*/

  }

  // 支付信息实体类
  case class PaymentInfo(
                          id: Long,
                          order_id: Long,
                          user_id: Long,
                          total_amount: BigDecimal,
                          subject: String,
                          payment_type: String,
                          create_time: String,
                          callback_time: String
                        )

  // 支付宽表实体类
  case class PaymentWide(
                          var payment_id: Long = 0L,

                          var subject: String = null,
                          var payment_type: String = null,
                          var payment_create_time: String = null,
                          var callback_time: String = null,
                          var detail_id: Long = 0L,
                          var order_id: Long = 0L,
                          var sku_id: Long = 0L,
                          var order_price: BigDecimal = null,
                          var sku_num: Long = 0L,
                          var sku_name: String = null,
                          var province_id: Long = 0L,
                          var order_status: String = null,
                          var user_id: Long = 0L,
                          var total_amount: BigDecimal = null,
                          var activity_reduce_amount: BigDecimal = null,
                          var coupon_reduce_amount: BigDecimal = null,
                          var original_total_amount: BigDecimal = null,
                          var feight_fee: BigDecimal = null,
                          var split_feight_fee: BigDecimal = null,
                          var split_activity_amount: BigDecimal = null,
                          var split_coupon_amount: BigDecimal = null,
                          var split_total_amount: BigDecimal = null,
                          var order_create_time: String = null,

                          var province_name: String = null, //查询维表得到

                          var province_area_code: String = null,
                          var province_iso_code: String = null,
                          var province_3166_2_code: String = null,
                          var user_age: Int = 0,
                          var user_gender: String = null,

                          var spu_id: Long = 0L, //作为维度数据 要关联进来

                          var tm_id: Long = 0L,
                          var category3_id: Long = 0L,
                          var spu_name: String = null,
                          var tm_name: String = null,
                          var category3_name: String = null
                        ) {

    def mergePaymentInfo(paymentInfo: PaymentInfo): PaymentWide = {
      if (paymentInfo != null) {
        this.payment_create_time = paymentInfo.create_time
        this.payment_id = paymentInfo.id
      }
      this
    }

    def mergeOrderWide(orderWide: OrderWide): PaymentWide = {
      if (orderWide != null) {
        this.order_create_time = orderWide.create_time
      }
      this
    }

  }

  // 访客统计实体类  包括各个维度和度量
  case class VisitorStats(
                           //统计开始时间
                           var stt: String = null,
                           //统计结束时间
                           var edt: String = null,
                           //维度：版本
                           vc: String = null,
                           //维度：渠道
                           ch: String = null,
                           //维度：地区
                           ar: String = null,
                           //维度：新老用户标识
                           is_new: String = null,
                           //度量：独立访客数
                           var uv_ct: Long = 0L,
                           //度量：页面访问数
                           var pv_ct: Long = 0L,
                           //度量： 进入次数
                           var sv_ct: Long = 0L,
                           //度量： 跳出次数
                           var uj_ct: Long = 0L,
                           //度量： 持续访问时间
                           var dur_sum: Long = 0L,
                           //统计时间
                           ts: Long = 0L
                         )

  // TransientSink 用该注解标记的属性，不需要插入到ClickHouse
  @Target(FIELD)
  @Retention(RUNTIME)
  trait TransientSink

  // 商品统计实体类
  case class ProductStats(
                           var stt: String = null, //窗口起始时间
                           var edt: String = null, //窗口结束时间
                           var sku_id: Long = 0L, //sku编号
                           var sku_name: String = null, //sku名称
                           var sku_price: BigDecimal = null, //sku单价
                           var spu_id: Long = 0L, //spu编号
                           var spu_name: String = null, //spu名称
                           var tm_id: Long = 0L, //品牌编号
                           var tm_name: String = null, //品牌名称
                           var category3_id: Long = 0L, //品类编号
                           var category3_name: String = null, //品类名称

                           var display_ct: Long = 0L, //曝光数
                           var click_ct: Long = 0L, //点击数
                           var favor_ct: Long = 0L, //收藏数
                           var cart_ct: Long = 0L, //添加购物车数
                           var order_sku_num: Long = 0L, //下单商品个数
                           var order_amount: BigDecimal = null, //下单商品金额
                           var order_ct: Long = 0L, //订单数
                           var payment_amount: BigDecimal = null, //支付金额
                           var paid_order_ct: Long = 0L, //支付订单数
                           var refund_order_ct: Long = 0L, //退款订单数
                           var refund_amount: BigDecimal = null,
                           var comment_ct: Long = 0L, //评论数
                           var good_comment_ct: Long = 0L, //好评数

                           @TransientSink
                           orderIdSet: util.HashSet[Long] = new util.HashSet[Long](), //用于统计订单数

                           @TransientSink
                           paidOrderIdSet: util.HashSet[Long] = new util.HashSet[Long](), //用于统计支付订单数

                           @TransientSink
                           refundOrderIdSet: util.HashSet[Long] = new util.HashSet[Long](), //用于退款支付订单数

                           var ts: Long = 0L //统计时间戳
                         )

  // 地区统计宽表实体类
  case class ProvinceStats(
                            var stt: String = null,
                            var edt: String = null,
                            var province_id: Long = 0L,
                            var province_name: String = null,
                            var area_code: String = null,
                            var iso_code: String = null,
                            var iso_3166_2: String = null,
                            var order_amount: BigDecimal = null,
                            var order_count: Long = 0L,
                            var ts: Long = 0L
  ){
    def mergeOrderWide(orderWide: OrderWide): ProvinceStats = {
      if (orderWide != null) {
        province_id = orderWide.province_id
        order_amount = orderWide.split_total_amount
        province_name = orderWide.province_name
        area_code = orderWide.province_area_code
        iso_3166_2 = orderWide.province_iso_code
        iso_code = orderWide.province_iso_code

        order_count = 1L
        ts = new Date().getTime
      }
      this
    }
  }

  // 关键词统计实体类
  case class KeywordStats(
                           keyword: String,
                           ct: Long,
                           source: String,
                           stt: String,
                           edt: String,
                           ts: Long
                         )

}