package com.tangxc.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:59 2018/11/6
 * @Modified by:
 */
public class MockDataUtil {

    /**
     * 搜索关键词
     */
    private static final String[] searchKeywords = new String[] {
            "火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
            "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"
    };

    /**
     * 访问行为
     */
    private static final String[] actions = new String[] {
            "search", "click", "order", "pay"
    };


    /**
     * 生成模拟数据
     * @param jsc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {

    }

    /**
     * 模拟生成用户访问行为数据，包含字段如下：
     *      date、user_id、session_id、page_id、action_time、search_keyword、click_category_id、click_product_id
     *      order_category_ids、order_product_ids、pay_category_ids、pay_product_ids、city_id
     */
    private static void mockUserVisitActionData(Random random, String date) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i ++) {
            // 会员ID
            long userId = random.nextInt(100);
            for (int j = 0; j < 10; j ++) {
                // sessionID
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(23));

                // 点击类别ID
                Long clickCategoryId = null;
                for (int k = 0; k < 100; k ++) {
                    // 页面ID
                    long pageId = random.nextInt(10);
                    // 访问时间
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(59))
                            + StringUtils.fulfuill(random.nextInt(59));
                    // 搜索词
                    String searchKeyword = null;
                    // 点击商品ID
                    Long clickProductId = null;
                    // 订单类别ID
                    String orderCategoryIds = null;
                    // 订单商品ID
                    String orderProductIds = null;
                    // 支付类别ID
                    String payCategoryIds = null;
                    // 支付商品ID
                    String payProductIds = null;

                    // 获取访问行为
                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        if(clickCategoryId == null) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }
                    Row row = RowFactory.create(date, userId, sessionId, pageId,
                            actionTime, clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds, payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));
                    rows.add(row);
                }
            }
        }
//        JavaRDD<Row> rowsRDD =
    }

    private static void mockUserInfoData() {

    }

    private static void mockProductInfoData() {

    }

}
