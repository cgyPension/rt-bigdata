package com.realtime.app.udf

import com.realtime.utils.KeywordUtil
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import scala.collection.JavaConversions._
import java.util

/**
 * Desc: 自定义UDTF函数实现分词功能
 *参考https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/functions/udfs.html
 *@FunctionHint 主要是为了标识输出数据的类型
 *row.setField(0,keyword)中的0表示返回值下标为0的值
 */
@FunctionHint(output = new DataTypeHint("ROW<s STRING>"))
object KeywordUDTF extends TableFunction[Row] {

  def eval(value: String): Unit = {
    val keywordList: List[String] = KeywordUtil.analyze(value)

    for (keyword <- keywordList) {
      val row: Row = new Row(1)
      row.setField(0, keyword)
      collect(row)
    }
  }
}