
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
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.regex.Pattern;

/**
 * 数字工具类
 *
 * @author liguoyang
 */
public class NumberUtils {

	/**
	 * 保留两位小数
	 */
	public static String f2(Number number) {
		DecimalFormat decimalFormat = new DecimalFormat("#.##");
		return decimalFormat.format(number);
	}

	/**
	 * 去除小数位
	 */
	public static String f0(Number number) {
		DecimalFormat decimalFormat = new DecimalFormat("#");
		return decimalFormat.format(number);
	}

	/**
	 * 两位整数
	 */
	public static String i2(Number number) {
		DecimalFormat decimalFormat = new DecimalFormat("00");
		return decimalFormat.format(number);
	}

	/**
	 * 数字大小比较
	 *
	 * @param n1
	 *            数字1
	 * @param n2
	 *            数字2
	 * @return
	 */
	public static int compare(Number n1, Number n2) {
		if (n1 == null) {
			return -1;
		}
		if (n2 == null) {
			return 0;
		}
		if (n1 instanceof Integer) {
			Integer i1 = new Integer(n1.toString());
			Integer i2 = new Integer(n2.toString());
			if (i1.equals(i2)) {
				return 0;
			}
			return i1.compareTo(i2);
		}
		if (n1 instanceof Long) {
			Long i1 = new Long(n1.toString());
			Long i2 = new Long(n2.toString());
			if (i1.equals(i2)) {
				return 0;
			}
			return i1.compareTo(i2);
		}
		if (n1 instanceof Float) {
			Float i1 = new Float(n1.toString());
			Float i2 = new Float(n2.toString());
			if (i1.equals(i2)) {
				return 0;
			}
			return i1.compareTo(i2);
		}
		if (n1 instanceof Double) {
			Double i1 = new Double(n1.toString());
			Double i2 = new Double(n2.toString());
			if (i1.equals(i2)) {
				return 0;
			}
			return i1.compareTo(i2);
		}
		return 0;
	}

	/**
	 * 判断是否是数字
	 *
	 * @param str
	 * @return
	 */
	public static boolean isInteger(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(str).matches();
	}

	/**
	 * 百分比
	 *
	 * @param num1
	 *            数1
	 * @param num2
	 *            数2
	 * @param scale
	 *            保留小数位
	 * @return 百分比
	 */
	public static String accuracy(double num1, double num2, int scale) {

		DecimalFormat df = (DecimalFormat) NumberFormat.getInstance();
		// 可以设置精确几位小数
		df.setMaximumFractionDigits(scale);
		// 模式 例如四舍五入
		df.setRoundingMode(RoundingMode.HALF_UP);
		df.applyPattern("##.##");
		if (num2 == 0) {
			return df.format(0);
		} else {
			double accuracy_num = num1 / num2 * 100;
			return df.format(accuracy_num);
		}
	}

	/**
	 * Object转换double
	 * @param obj 对象
	 * @return double
	 */
	public static double parseDouble(Object obj){
		if(obj == null || StringUtils.isNull(obj.toString())){
			return 0d;
		}else {
			return Double.parseDouble(obj.toString());
		}
	}

	/**
	 * Object转换int
	 * @param obj 对象
	 * @return int
	 */
	public static int parseInt(Object obj){
		if(obj == null || StringUtils.isNull(obj.toString())){
			return 0;
		}else {
			return Integer.parseInt(obj.toString());
		}
	}

	/**
	 * Object转换long
	 * @param obj 对象
	 * @return long
	 */
	public static long parseLong(Object obj){
		if(obj == null || StringUtils.isNull(obj.toString())){
			return 0;
		}else {
			return Long.parseLong(obj.toString());
		}
	}


	 /**
	  * 格式化
	  * @param data
	  * @param length 强制保留长度
	  * @return
	  */
	 public static String dataFormat(String data,int length) {
			if (null == data || data.trim().length() == 0) {
				data = "0";
			}
			DecimalFormat df = null;
			// 如果是四位
			if(length==4){
				df = new DecimalFormat("0.0000");
			}else if(length==12){ // 如果是12位
				df = new DecimalFormat("0.000000000000");
		} else if (length == 2) {
			df = new DecimalFormat("0.00");
			}
			BigDecimal b1 = new BigDecimal(data);
			return df.format(b1);
		}
}
