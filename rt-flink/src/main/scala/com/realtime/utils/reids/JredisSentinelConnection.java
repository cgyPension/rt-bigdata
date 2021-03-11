package com.realtime.utils.reids;

import com.realtime.conf.ConfigurationManager;
import com.realtime.conf.Constants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class JredisSentinelConnection {
    /**
     * 连接池连接方式
     * @return
     */
    private static Logger logger = LogManager.getLogger(JredisSentinelConnection.class.getName());
    public static Jedis GetRedisConnection(String env){
        String[] hosts = null;
        String password = null;
        if (env.equals("prod")) {
            hosts = ConfigurationManager.getProperties(Constants.REDIS_SENTINEL_HOSTS_PROD).split(",");
            password = ConfigurationManager.getProperties(Constants.REDIS_SERVER_PASSWORD_PROD);
        } else if (env.equals("dev")) {
            hosts = ConfigurationManager.getProperties(Constants.REDIS_SENTINEL_HOSTS).split(",");
            password = ConfigurationManager.getProperties(Constants.REDIS_SERVER_PASSWORD);
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
        String masterName = ConfigurationManager.getProperties(Constants.REDIS_SENTINEL_MASTER_NAME);
        Set<String> sentinels = new HashSet<>(Arrays.asList(hosts));
        JedisSentinelPool jedisPool = new JedisSentinelPool(masterName, sentinels, config, timeOut, password);

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