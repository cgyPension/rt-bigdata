package com.realtime.app.udf



import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.functions._
import org.apache.flink.types.Row
import com.realtime.conf.GmallConstant

// 自定义UDTF函数实现商品点击次数、订单次数、添加购物次数的统计
@FunctionHint(output = new DataTypeHint("ROW<ct BIGINT,source STRING>"))
object KeywordProductC2RUDTF {

  def eval(clickCt: Long, cartCt: Long, orderCt: Long): Unit = {
    if (clickCt > 0L) {
      val rowClick: Row = new Row(2)
      rowClick.setField(0, clickCt)
      rowClick.setField(1, GmallConstant.KEYWORD_CLICK)
     collect(rowClick)
    }
    if (cartCt > 0L) {
      val rowCart: Row = new Row(2)
      rowCart.setField(0, cartCt)
      rowCart.setField(1, GmallConstant.KEYWORD_CART)
      collect(rowCart)
    }
    if (orderCt > 0) {
      val rowOrder: Row = new Row(2)
      rowOrder.setField(0, orderCt)
      rowOrder.setField(1, GmallConstant.KEYWORD_ORDER)
      collect(rowOrder)
    }
  }


}
