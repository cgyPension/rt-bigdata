package com.realtime.common;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OperatorOnSqlServer {
    // Log4j 记录日志
    private static Logger logger = LogManager.getLogger(OperatorOnSqlServer.class.getName());
    private static PreparedStatement ps=null;
    private static ResultSet rs=null;
    // 关闭
    public static void close(Connection conn, PreparedStatement ps) {
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static PreparedStatement setParam(PreparedStatement ps,Object[] params) {
        //循环设置参数
        if(params!=null){
            for (int i = 0; i < params.length; i++) {
                try {
                    ps.setObject(i+1, params[i]);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return ps;
    }
    // 更新
    public static boolean executeUpdate(Connection conn, String sql,Object[] params) {
        try {
            ps=conn.prepareStatement(sql);
            ps=setParam(ps, params);
            int ret=ps.executeUpdate();
            if(ret>0){
                System.out.println("操作成功！");
                return true;
            }else{
                System.out.println("操作失败！");
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            close(conn, ps);
        }
        return false;
    }
    // 查询SQL
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql, Object[] params){
        List<Map<String, Object>> objectList=new ArrayList<>();
        try{
            ps=conn.prepareStatement(sql);
            ps=setParam(ps, params);
            rs=ps.executeQuery();
            ResultSetMetaData rsmd=rs.getMetaData();
            while(rs.next()){
                Map<String, Object> rowMap=new LinkedHashMap<>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {//循环遍历所有列
                    rowMap.put(rsmd.getColumnName(i+1), rs.getObject(i+1));
                }
                objectList.add(rowMap);
            }
        }catch(SQLException e){
            e.printStackTrace();
        }finally{
            close(conn, ps);
        }
        return objectList;
    }

}
