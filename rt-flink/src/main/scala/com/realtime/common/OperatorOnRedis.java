package com.realtime.common;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

public class OperatorOnRedis {
    /**
     * 向缓存中设置字符串内容
     * @param key key
     * @param value value
     * @return
     * @throws Exception
     */

    public static boolean  set(Jedis jredis, int db, String key, String value){
        try {
            jredis.select(db);
            jredis.set(key, value);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 向缓存中设置对象
     * @param key
     * @param value
     * @return
     */
    public static boolean setJSONObject(Jedis jredis, int db, String key,JSONObject value){
        try {
            jredis.select(db);
            jredis.set(key, value.toJSONString());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 向缓存中设置对象(带有秒级别存货周期的)
     * @param key
     * @param value
     * @return
     */
    public static boolean setJSONObject(Jedis jredis, int db, String key,JSONObject value, Long milliseconds){
        try {
            jredis.select(db);
            jredis.psetex(key, milliseconds, value.toJSONString());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 向缓存中设置对象(带有毫秒级别存货周期的)
     * @param key
     * @param value
     * @return
     */
    public static boolean setJSONObject(Jedis jredis, int db, String key,JSONObject value, int seconds){
        try {
            jredis.select(db);
            jredis.setex(key, seconds, value.toJSONString());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除缓存中得对象，根据key
     * @param key
     * @return
     */
    public static boolean del(Jedis jredis, int db, String key){
        try {
            jredis.select(db);
            jredis.del(key);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 根据key 获取内容
     * @param key
     * @return
     */
    public static JSONObject getJSONObject(Jedis jredis, int db, String key){
        try {
            jredis.select(db);
            JSONObject value = JSONObject.parseObject(jredis.get(key));
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据key 获取内容
     * @param key
     * @return
     */
    public static Object get(Jedis jredis, int db, String key){
        try {
            jredis.select(db);
            Object value = jredis.get(key);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据key 获取对象
     * @param key
     * @return
     */
    public static <T> T get(Jedis jredis, int db, String key, Class<T> clazz){
        try {
            jredis.select(db);
            String value = jredis.get(key);
            return JSONObject.parseObject(value, clazz);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 设置Key的存货周期
     * @param key
     * @return
     */
    public static boolean pexpire(Jedis jredis, int db, String key, long milliseconds){
        try {
            jredis.select(db);
            jredis.pexpire(key, milliseconds);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
