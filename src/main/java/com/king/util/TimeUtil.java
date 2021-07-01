package com.king.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-07-01 20:08
 */
public class TimeUtil {
    public static Date parse(String time, String format) {
        DateFormat formatter = new SimpleDateFormat(format);

        try {
            return formatter.parse(time);
        } catch (ParseException e) {
            return new Date();
        }
    }

    /**
     * 格式化的时间转为时间戳
     *
     * @param time
     * @return
     */
    public static long parse(String time) {
        return parse(time, "yyyy-MM-dd HH:mm:ss").getTime();
    }
}
