package com.realtime.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

/**
 * @Classname WriteDataToClickhouse
 */

public class OperatorOnClickhouse {
    // Log4j 记录日志
    private static Logger logger = LogManager.getLogger(OperatorOnClickhouse.class.getName());

    public void executeBatch(Connection conn, String dbt, List<JSONObject> listParams, int batch) {
        Statement psmt = null;
        try {
            // 设置自动提交为false
            conn.setAutoCommit(false);
            psmt = conn.createStatement();

            try {
                int cnt = 0 ;
                int count = 0 ;
                Iterator<JSONObject> it = listParams.iterator();
                int dataSize = listParams.size();
                synchronized(it) {
                    StringBuffer sb = new StringBuffer();
                    while (it.hasNext()) {
                        JSONObject data = it.next();
                        try {
                            sb.append(data.toJSONString());
                        } catch (Exception e) {
                            logger.error("StringBuffer 异常");
                        }
                        // 迭代器计数+1
                        count++;
                        if ((batch * (cnt + 1) <= count || dataSize==count) && sb.length() > 0) {
                            cnt++; // 符合批次的计数+1

                            String insert_sql = "INSERT INTO " + dbt + " FORMAT JSONEachRow " + sb.toString();
                            sb.setLength(0); // 清楚已插入数据库部分

                            try {
                                psmt.executeQuery(insert_sql);
                                conn.commit();
                            } catch (Exception e) {
                                conn.rollback();
                                logger.error("插入数据异常*****************************************\n" + e.getLocalizedMessage() +"\n"+
                                        "dbt=" + dbt + ": 总条数=" + listParams.size() + "; batch=" + batch + "; 批次:" + cnt + "; 累计计数:" + count);
                            }
                        }
                    }
                }
            } catch (Exception e){
                logger.error(";dbt:"+dbt+"; 迭代器迭代异常 >> "+ e.getMessage());
            }
            conn.close();
        } catch (Exception e) {
            logger.error("连接数据库异常: "+e.toString()+ e.getMessage());
        }
    }
}
