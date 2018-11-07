package com.tangxc.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:17 2018/11/7
 * @Modified by:
 */
public class DateUtils {

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    public static String getTodayDate() {
        return getTodayDate(DATE_FORMAT);
    }

    public static String getTodayDate(String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        return simpleDateFormat.format(new Date());
    }
}
