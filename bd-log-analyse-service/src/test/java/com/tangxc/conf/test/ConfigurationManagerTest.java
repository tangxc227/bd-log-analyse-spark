package com.tangxc.conf.test;

import com.tangxc.conf.ConfigurationManager;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:13 2018/11/7
 * @Modified by:
 */
public class ConfigurationManagerTest {

    @Test
    public void testGetProperty() {
        System.out.println(ConfigurationManager.getProperty("testkey1"));
    }

}
