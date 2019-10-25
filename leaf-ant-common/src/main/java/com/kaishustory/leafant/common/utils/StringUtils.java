
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kaishustory.leafant.common.utils;


import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 字符串工具类
 */
public class StringUtils {

    /**
     * 对象转为字符串
     *
     * @param val 对象
     * @return 字符串
     */
    public static String toString(Object val) {
        if (val == null) {
            return "";
        } else if (val instanceof Number) {
            BigDecimal bigDecimal = new BigDecimal(val.toString());
            String number = bigDecimal.toString();
            if (number.endsWith(".0")) {
                number = number.substring(0, number.indexOf("."));
            }
            return number;
        } else if (val instanceof Date) {
            String date = DateUtils.toTimeString((Date) val);
            if (date.endsWith("00:00:00")) {
                return date.substring(0, 10);
            } else {
                return date;
            }
        } else {
            return val.toString();
        }
    }

    /**
     * 数组转String
     *
     * @param val 数组
     * @return 字符串
     */
    public static String toString(String[] val) {
        StringBuffer str = new StringBuffer();
        for (int i = 0; i < val.length; i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append(val[i]);
        }
        return str.toString();
    }

    /**
     * 数组转String
     *
     * @param val 数组
     * @return 字符串
     */
    public static String toString(List<String> val) {
        StringBuffer str = new StringBuffer();
        for (int i = 0; i < val.size(); i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append(val.get(i));
        }
        return str.toString();
    }

    /**
     * 空字符串处理
     *
     * @param str 字符串
     * @return null转为空字符串
     */
    public static String nullStr(String str) {
        if (str == null) {
            return "";
        } else {
            return str;
        }
    }

    /**
     * is true
     *
     * @param bool
     * @return
     */
    public static boolean isTrue(Boolean bool) {
        return (bool != null && bool);
    }

    /**
     * is false
     *
     * @param bool
     * @return
     */
    public static boolean isFalse(Boolean bool) {
        return (bool == null || !bool);
    }

    /**
     * is not null
     *
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotNull(String str) {
        return str != null && str.trim().length() > 0 && !str.equals("null");
    }

    /**
     * is null
     *
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNull(String str) {
        return str == null || str.trim().length() == 0 || str.equals("null");
    }

    /**
     * 提取相同字符串列表
     *
     * @param array1 字符串列表1
     * @param array2 字符串列表2
     * @return 相同字符串列表
     */
    public static List<String> some(List<String> array1, List<String> array2) {
        List<String> some = new ArrayList<String>();

        int a1_size = array1.size();
        int a2_size = array2.size();
        for (int i = 0; i < a1_size; i++) {
            for (int j = 0; j < a2_size; j++) {
                if (array1.get(i) == null || array2.get(j) == null) {
                    continue;
                }
                if (array1.get(i).equals(array2.get(j))) {
                    some.add(array1.get(i));
                    array2.remove(j);
                    a2_size--;
                    j--;
                    break;
                }
            }
        }
        return some;
    }

    /**
     * md5加密
     *
     * @param sourceStr
     * @return
     */
    public static String MD5(String sourceStr) {
        String result = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(sourceStr.getBytes());
            byte[] b = md.digest();
            int i;
            StringBuffer buf = new StringBuffer();
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e);
        }
        return result;
    }
}
