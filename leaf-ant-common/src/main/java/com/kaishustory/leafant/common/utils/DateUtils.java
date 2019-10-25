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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 日期工具类
 *
 * @author liguoyang
 */
public class DateUtils {

    /**
     * 当前日期
     */
    public static Date now() {
        return new Date();
    }

    /**
     * 今日日期
     */
    public static String today() {
        return DateUtils.toDateString(DateUtils.now());
    }

    /**
     * 昨日日期
     */
    public static String yesterday() {
        return DateUtils.toDateString(DateUtils.addTime(DateUtils.now(), Calendar.DATE, -1));
    }

    /**
     * 比较日期(计算相差毫秒数)
     *
     * @param dateA 日期A
     * @param dateB 日期B
     * @return 0: 日期A=日期B, >0: 日期A>日期B, <0: 日期A<日期B
     */
    public static int compare(String dateA, String dateB) {
        return (toDate(dateA).getTime() - toDate(dateB).getTime()) > 0 ? 1 : -1;
    }

    /**
     * 比较日期(计算相差毫秒数)
     *
     * @param dateA 日期A
     * @param dateB 日期B
     * @return 0: 日期A=日期B, >0: 日期A>日期B, <0: 日期A<日期B
     */
    public static int compare(Date dateA, Date dateB) {
        return (dateA.getTime() - dateB.getTime()) > 0 ? 1 : -1;
    }

    /**
     * 获得周几
     *
     * @param date 日期
     * @return 周几
     */
    public static int week(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day_of_week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (day_of_week == 0) {
            day_of_week = 7;
        }
        return day_of_week;
    }

    /**
     * 获得周一日期
     */
    public static String weekfirstday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day_of_week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (day_of_week == 0) {
            day_of_week = 7;
        }
        calendar.add(Calendar.DATE, -day_of_week + 1);
        return DateUtils.toDateString(calendar.getTime());
    }

    /**
     * 获得周末日期
     */
    public static String weeklastday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day_of_week = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (day_of_week == 0) {
            day_of_week = 7;
        }
        calendar.add(Calendar.DATE, 7 - day_of_week);
        return DateUtils.toDateString(calendar.getTime());
    }

    /**
     * 获得月第一天日期
     */
    public static String monthfirstday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return DateUtils.toDateString(calendar.getTime());
    }

    /**
     * 获得月最后一天日期
     */
    public static String monthlastday(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return DateUtils.toDateString(calendar.getTime());
    }

    /**
     * 获得时间范围内日期列表
     *
     * @param beginDate 开始日期
     * @param endDate   截止日期
     * @return 日期列表
     */
    public static List<String> getRangeDateList(String beginDate, String endDate) {
        //转换Date
        Date b_date = DateUtils.toDate(beginDate);
        Date e_date = DateUtils.toDate(endDate);
        List<String> dateList = new ArrayList<String>();
        do {
            //记录日期
            dateList.add(DateUtils.toDateString(b_date));
            //日期加1
            b_date = DateUtils.addDate(b_date, 1);
            //直至达到截止日期
        } while (DateUtils.compare(b_date, e_date) <= 0);
        return dateList;
    }

    /**
     * 时间转字符串
     *
     * @param date 时间
     * @return 字符串（yyyy-MM-dd HH:mm:ss）
     */
    public static String toTimeString(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(date);
    }

    /**
     * 日期转字符串
     *
     * @param date 日期
     * @return 字符串（yyyy-MM-dd）
     */
    public static String toDateString(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

    /**
     * 字符串转时间
     *
     * @param dateStr 字符串
     * @return 时间
     */
    public static Date toTime(String dateStr) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            Log.error("日志转换异常!", e);
        }
        return null;
    }

    /**
     * 字符串转日期
     *
     * @param dateStr 字符串
     * @return 日期
     */
    public static Date toDate(String dateStr) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            Log.error("日志转换异常!", e);
        }
        return null;
    }

    /**
     * 计算时间
     *
     * @param date      日期
     * @param dateField 计算单位（Calendar.DATE：日,Calendar.HOUR：小时...）
     * @param num       数值（正值：加，负值：减）
     * @return
     */
    public static Date addTime(Date date, int dateField, int num) {
        Calendar calendar = Calendar.getInstance();
        if (calendar == null || date == null) {
            Log.info(date.toString());
        }
        calendar.setTime(date);
        calendar.add(dateField, num);
        return calendar.getTime();
    }


    /**
     * 计算日期
     *
     * @param date 日期
     * @param day  加天数（正值：加，负值：减）
     * @return 日期
     */
    public static Date addDate(Date date, int day) {
        return addTime(date, Calendar.DATE, day);
    }

    /**
     * 计算日期
     *
     * @param date 日期
     * @param day  加天数（正值：加，负值：减）
     * @return 日期
     */
    public static String addDate(String date, int day) {
        return toDateString(addTime(toDate(date), Calendar.DATE, day));
    }

    /**
     * 范围日期列表
     *
     * @param date 日期
     * @param day  加天数（正值：加，负值：减）
     * @return 日期列表
     */
    public static List<String> rangeDate(String date, int day) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < day; i++) {
            list.add(toDateString(addTime(toDate(date), Calendar.DATE, i)));
        }
        return list;
    }

    /**
     * 日期计算
     *
     * @param date      日期
     * @param dateField 计算单位（Calendar.DATE：日,Calendar.HOUR：小时...）
     * @param num       数值（正值：加，负值：减）
     * @return
     */
    public static String addTime(String date, int dateField, int num) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(toTime(date));
        calendar.add(dateField, num);
        return toTimeString(calendar.getTime());
    }

    /**
     * 获得星期几
     *
     * @param date 日期
     * @return 星期几
     */
    public static int getWeek(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int week = calendar.get(Calendar.DAY_OF_WEEK);
        if (week == 1) {
            week = 7;
        } else {
            week -= 1;
        }
        return week;
    }

    /**
     * 获得月份
     *
     * @param date 日期
     * @return 月份
     */
    public static int getMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int month = calendar.get(Calendar.MONTH) + 1;
        return month;
    }

    /**
     * 加8小时
     *
     * @param time 时间
     * @return 修正时间
     */
    public static String add8Hour(String time) {
        if (time != null) {
            // 减8小时
            time = toTimeString(addTime(toTime(time), Calendar.HOUR, 8));
        }
        return time;
    }

    /**
     * 减8小时
     *
     * @param time 时间
     * @return 修正时间
     */
    public static String sub8Hour(String time) {
        if (time != null) {
            // 减8小时
            time = toTimeString(addTime(toTime(time), Calendar.HOUR, -8));
        }
        return time;
    }

}
