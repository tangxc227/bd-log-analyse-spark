package com.tangxc.conf;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:07 2018/11/7
 * @Modified by:
 */
public class ConfigurationManager {

    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);

    private static Properties properties = new Properties();

    static {
        try {
            InputStream inputStream = ConfigurationManager.class.getClassLoader()
                    .getResourceAsStream("log_analyse.properties");
            properties.load(inputStream);
            logger.info("加载配置文件成功");
        } catch (Exception e) {
            logger.error("加载配置文件失败", e);
            throw new RuntimeException("加载配置文件失败", e);
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static int getInt(String key) {
        try {
            return Integer.valueOf(getProperty(key));
        } catch (Exception e) {
            throw new RuntimeException("cast string convert to int failed:", e);
        }
    }

    public static boolean getBoolean(String key) {
        try {
            return Boolean.valueOf(getProperty(key));
        } catch (Exception e) {
            throw new RuntimeException("cast string convert to boolean failed:", e);
        }
    }

}
