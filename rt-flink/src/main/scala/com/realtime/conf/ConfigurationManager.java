package com.realtime.conf;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理类组件
 */
public class ConfigurationManager {
    private static Properties properties=new Properties();

    static {
        try {
            InputStream input=ConfigurationManager.class.
                    getClassLoader().getResourceAsStream("common.properties");
            properties.load(input);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  static String getProperties(String key) {
        return properties.getProperty(key);
    }
    public static Integer getInteger(String key) {
        try {
            return Integer.valueOf(getProperties(key));
        }catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 获取boolean类型常量
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        String value=properties.getProperty(key);
        return Boolean.valueOf(value);
    }

    /**
     * 获取Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperties(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

}
