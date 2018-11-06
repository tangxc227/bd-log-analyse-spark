package com.tangxc.util;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 17:22 2018/11/6
 * @Modified by:
 */
public class StringUtils {

    /**
     * 将整数转化为字符串，并且不足2位的，在前面补0
     * @param value
     * @return
     */
    public static String fulfuill(int value) {
        return String.format("%02d", value);
    }

}
