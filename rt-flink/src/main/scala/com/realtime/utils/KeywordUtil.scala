package com.realtime.utils

import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme

import java.io.{IOException, StringReader}
import java.sql.PreparedStatement
import java.util
import scala.collection.JavaConversions.seqAsJavaList
import scala.text.Document.break
import scala.util.control.Breaks.breakable
import scala.collection.JavaConversions._

// IK分词器
object KeywordUtil {

  //使用IK分词器对字符串进行分词//使用IK分词器对字符串进行分词
  def analyze(text: String)= {
    val sr: StringReader = new StringReader(text)
    val ik: IKSegmenter = new IKSegmenter(sr, true)
    var lex: Lexeme = null
//    val keywordList: util.List[String] = new util.List[String]
    val keywordList = List[String]()  // java的List

    while (true) {
      try {
        breakable{
          if ((lex = ik.next) != null) {
            val lexemeText: String = lex.getLexemeText
            keywordList.add(lexemeText)
          }else break
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          throw new RuntimeException("分词失败！！！")
        }
      }
    }

    keywordList
  }


  def main(args: Array[String]): Unit = {
    val text: String = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待"
    println(KeywordUtil.analyze(text))
  }
}
