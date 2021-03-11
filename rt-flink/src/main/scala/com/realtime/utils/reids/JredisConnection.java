package com.realtime.utils.reids;

import com.realtime.conf.ConfigurationManager;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static com.realtime.conf.Constants.*;



public class JredisConnection {
    /**
     * 连接池连接方式
     * @return
     */
    private static Logger logger = LogManager.getLogger(JredisConnection.class.getName());
    public static Jedis GetRedisConnection(String env){
        String host = null;
        Integer port = null;
        String password = null;
        if (env.equals("prod")) {
            host = ConfigurationManager.getProperties(REDIS_SERVER_HOST_PROD);
            port = ConfigurationManager.getInteger(REDIS_SERVER_PORT_PROD);
            password = ConfigurationManager.getProperties(REDIS_SERVER_PASSWORD_PROD);
        } else if (env.equals("dev")) {
            host = ConfigurationManager.getProperties(REDIS_SERVER_HOST);
            port = ConfigurationManager.getInteger(REDIS_SERVER_PORT);
            password = ConfigurationManager.getProperties(REDIS_SERVER_PASSWORD);
        } else {
            logger.error("获取环境出现错误值: " + env);
            System.exit(0);
        }

        // 获取连接池配置对象
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100); //最大连接数
        config.setMaxIdle(20); //最大空闲
        config.setMinIdle(20); //最小空闲
        config.setBlockWhenExhausted(true); //忙碌时是否等待
        config.setMaxWaitMillis(500); //忙碌时等待时长 毫秒
        config.setTestOnBorrow(false); //每次获得连接的进行测试

        int timeOut = 6000;
        JedisPool jedisPool = new JedisPool(config, host, port, timeOut, password, 0);

        // 获得核心对象：jedis
        Jedis jedis = null;
        try{
            // 通过连接池来获得连接
            jedis = jedisPool.getResource();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            // 释放资源
            if(jedis != null){
                jedis.close();
            }
            // 释放连接池
            if(jedisPool != null){
                jedisPool.close();
            }
        }
        return jedis;
    }

}