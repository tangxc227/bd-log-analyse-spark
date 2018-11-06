package com.tangxc.util.test;

import com.tangxc.util.StringUtils;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 17:26 2018/11/6
 * @Modified by:
 */
public class StringUtilsTest {

    @Test
    public void testFulfuill() {
        int num = 2;
        System.out.println(StringUtils.fulfuill(num));
        num = 12;
        System.out.println(StringUtils.fulfuill(num));
    }

}
