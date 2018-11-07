package com.tangxc.util.test;

import com.tangxc.util.MockDataUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:47 2018/11/7
 * @Modified by:
 */
public class MockDataUtilsTest {

    @Test
    public void testMockData() {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("mock_data");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc.sc());
        MockDataUtils.mockData(jsc, sqlContext);
    }

}
