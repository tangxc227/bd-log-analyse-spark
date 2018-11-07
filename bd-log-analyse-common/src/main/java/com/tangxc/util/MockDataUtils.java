package com.tangxc.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:59 2018/11/6
 * @Modified by:
 */
public class MockDataUtils {

    /**
     * 搜索关键词
     */
    private static final String[] searchKeywords = new String[]{
            "火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
            "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"
    };

    /**
     * 访问行为
     */
    private static final String[] actions = new String[]{
            "search", "click", "order", "pay"
    };


    /**
     * 生成模拟数据
     *
     * @param jsc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        Random random = new Random();
        String date = DateUtils.getTodayDate();
        // 模拟生成用户访问行为数据
        mockUserVisitActionData(jsc, sqlContext, random, date);
        // 模拟生成会员用户信息数据
        mockUserInfoData(jsc, sqlContext, random);
        // 模拟生成产品信息数据
        mockProductInfoData(jsc, sqlContext, random);
    }

    /**
     * 模拟生成用户访问行为数据，包含字段如下：
     * date、user_id、session_id、page_id、action_time、search_keyword、click_category_id、click_product_id
     * order_category_ids、order_product_ids、pay_category_ids、pay_product_ids、city_id
     *
     * @param jsc
     * @param sqlContext
     * @param random
     * @param date
     */
    private static void mockUserVisitActionData(JavaSparkContext jsc, SQLContext sqlContext, Random random, String date) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // 会员ID
            long userId = random.nextInt(100);
            for (int j = 0; j < 10; j++) {
                // sessionID
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(23));

                // 点击类别ID
                Long clickCategoryId = null;
                for (int k = 0; k < 100; k++) {
                    // 页面ID
                    long pageId = random.nextInt(10);
                    // 访问时间
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(59))
                            + ":" + StringUtils.fulfuill(random.nextInt(59));
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
                        if (clickCategoryId == null) {
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
                            actionTime, searchKeyword, clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds, payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));
                    rows.add(row);
                }
            }
        }
        JavaRDD<Row> rowsRDD = jsc.parallelize(rows);

        // 构建元数据
        StructType structType = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("user_id", DataTypes.LongType, true),
                        DataTypes.createStructField("session_id", DataTypes.StringType, true),
                        DataTypes.createStructField("page_id", DataTypes.LongType, true),
                        DataTypes.createStructField("action_time", DataTypes.StringType, true),
                        DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                        DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                        DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                        DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                        DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                        DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                        DataTypes.createStructField("city_id", DataTypes.LongType, true)
                )
        );
        // 创建DataFrame
        DataFrame userVisitActionDF = sqlContext.createDataFrame(rowsRDD, structType);
        // 注册临时表
        userVisitActionDF.registerTempTable("user_visit_action");

        sqlContext.sql("select * from user_visit_action limit 1").show();
    }

    /**
     * 模拟生成用户信息数据，包含字段如下：
     * user_id, username, name, age, professional, city, sex
     *
     * @param jsc
     * @param sqlContext
     * @param random
     */
    private static void mockUserInfoData(JavaSparkContext jsc, SQLContext sqlContext, Random random) {
        List<Row> rows = new ArrayList<>();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userId = i;
            String username = "user_" + i;
            String name = "name_" + i;
            int age = random.nextInt(60);
            String professional = "professional_" + random.nextInt(100);
            String city = "city_" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userId, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        JavaRDD<Row> userInfoRDD = jsc.parallelize(rows);

        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)
        ));

        sqlContext.createDataFrame(userInfoRDD, structType)
                .registerTempTable("user_info");

        sqlContext.sql("select * from user_info limit 1").show();
    }

    /**
     * 模拟生成产品信息数据，包含字段：
     * product_id，product_name，extend_info
     *
     * @param jsc
     * @param sqlContext
     * @param random
     */
    private static void mockProductInfoData(JavaSparkContext jsc, SQLContext sqlContext, Random random) {
        List<Row> rows = new ArrayList<>();
        int[] productStatus = new int[]{0, 1};
        for (int i = 0; i < 100; i++) {
            long productId = i;
            String productName = "product_" + i;
            String extendInfo = "{\"product_status\":" + productStatus[random.nextInt(2)] + "}";
            Row row = RowFactory.create(productId, productName, extendInfo);
            rows.add(row);
        }
        JavaRDD<Row> productInfoRDD = jsc.parallelize(rows);
        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)
        ));
        sqlContext.createDataFrame(productInfoRDD, structType)
                .registerTempTable("product_info");
        sqlContext.sql("select * from product_info limit 1").show();
    }

}
