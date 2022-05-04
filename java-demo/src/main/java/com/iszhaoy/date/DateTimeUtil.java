package com.iszhaoy.date;


import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Created by millions on 2018/4/11.
 */
//@Slf4j
public class DateTimeUtil {

    private static final String YYYY_MM_PATTERN = "yyyyMM";

    private static final String YYYYMMDD_PATTERN = "yyyyMMdd";

    private static final String YYYYMM_DD_POINT_PATTERN = "yyyy.MM.dd";

    private static final String YYYYMMDDHHMMSS_POINT_PATTERN = "yyyy.MM.dd HH:mm:ss";

    public static final String YYYY_MM_DD_PATTERN = "yyyy-MM-dd";

    private static final String YYYY_MM_DD_CHINESE_PATTERN = "yyyy年MM月dd日";

    private static final String MM_DD_CHINESE_PATTERN = "MM月dd日";

    private static final String MM_DD_HH_MM_CHINESE = "MM月dd日 HH:mm";

    private static final String YYYYMMDDHH_PATTERN = "yyyyMMddHH";

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    private static final String YYYYMMDD_HH_MM_SS = "yyyy.MM.dd HH:mm:ss";

    /**
     * 到小时分钟的日期格式.
     */
    public static final String FORMAT_DATETIME_HM = "yyyy-MM-dd HH:mm";


    public static final DateTimeFormatter YYYY_MM_FORMATTER = DateTimeFormatter.ofPattern(YYYY_MM_PATTERN);

    public static final DateTimeFormatter YYYYMMDD_FORMATTER = DateTimeFormatter.ofPattern(YYYYMMDD_PATTERN);

    public static final DateTimeFormatter YYYYMMDD_POINT_FOEMATTER =
            DateTimeFormatter.ofPattern(YYYYMM_DD_POINT_PATTERN);

    public static final DateTimeFormatter YYYYMMDDHHMMSS_POINT_FOEMATTER =
            DateTimeFormatter.ofPattern(YYYYMMDDHHMMSS_POINT_PATTERN);

    public static final DateTimeFormatter YYYY_MM_DD_FORMATTER = DateTimeFormatter.ofPattern(YYYY_MM_DD_PATTERN);

    public static final DateTimeFormatter YYYY_MM_DD_CHINESE_FORMATTER =
            DateTimeFormatter.ofPattern(YYYY_MM_DD_CHINESE_PATTERN);

    public static final DateTimeFormatter MM_DD_HH_MM_CHINESE_FORMATTER =
            DateTimeFormatter.ofPattern(MM_DD_HH_MM_CHINESE);

    public static final DateTimeFormatter MM_DD_CHINESE_FORMATTER = DateTimeFormatter.ofPattern(MM_DD_CHINESE_PATTERN);

    private static final DateTimeFormatter YYYYMMDDHH_FORMATTER = DateTimeFormatter.ofPattern(YYYYMMDDHH_PATTERN);

    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS_FROMATTER =
            DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);

    public static final DateTimeFormatter YYYYMMDD_HH_MM_SS_FROMATTER = DateTimeFormatter.ofPattern(YYYYMMDD_HH_MM_SS);

    public static final long MILLIS_OF_8_HOUR = TimeUnit.HOURS.toMillis(8);

    public static final long MILLIS_OF_DAY = TimeUnit.DAYS.toMillis(1);

    public static long computeZeroPointTimeStamp(long timestamp) {
        return timestamp - (timestamp + MILLIS_OF_8_HOUR) % MILLIS_OF_DAY;
    }

    public static long compute24PointTimeStamp(long timestamp) {
        long zeroTimeStamp = computeZeroPointTimeStamp(timestamp);
        return zeroTimeStamp + MILLIS_OF_DAY;
    }



    public static LocalDate strToLocalDate(String date) {
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern(YYYY_MM_DD_PATTERN);
        return LocalDate.parse(date, pattern);
    }

    public static LocalDateTime stringToLocalDateTime(String date) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        return LocalDateTime.parse(date, dateTimeFormatter);
    }

    public static LocalDateTime stringToLocalDateTime(String date, String pattern) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, dateTimeFormatter);
    }

    public static String getYueRi(Date date){
        SimpleDateFormat sdf = new SimpleDateFormat(MM_DD_CHINESE_PATTERN);
        return sdf.format(date);
    }


    /**
     * <p>Description:字符串转换为时间</p>
     *
     * @param date    日期字符串
     * @param pattern 转换模式
     * @return
     * @Title: string2Date
     * @author maqingrong
     */
    public static Date string2Date(String date, String pattern) {
        DateFormat df = new SimpleDateFormat(pattern);
        try {
            return df.parse(date);
        } catch (ParseException e) {
            //log.error("字符串转换为时间异常", e);
        }

        return null;
    }

    /**
     * 将日期格式化为字符串.<br>
     *
     * @param d 日期
     * @return 日期字符串
     */
    public static String getStringFromDate(Date d) {
        SimpleDateFormat sdf = new SimpleDateFormat();
        return sdf.format(d);
    }


    /**
     * 得到与指定日期相差指定天数的日期
     *
     * @param date 指定的日期
     * @param days 相差天数
     * @return
     */
    public static Date getCertainDate(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    public static Long strToTimeStamp(String date, String pattern) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, dateTimeFormatter).toEpochSecond(ZoneOffset.of("+8")) * 1000L;
    }


    public static Long strToTimeStamp(String date) {
        LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        LocalDateTime localDateTime = localDate.atStartOfDay();
        return localDateTime.toEpochSecond(ZoneOffset.of("+8")) * 1000L;
    }


    public static Long getTimeStamp(LocalDateTime dateTime) {
        return dateTime.toEpochSecond(ZoneOffset.of("+8")) * 1000L;
    }

    public static String dateToTimeString(LocalDateTime localDateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        return dateTimeFormatter.format(localDateTime);
    }

    public static String dateToTimeString(LocalDateTime localDateTime, String pattern) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
        return dateTimeFormatter.format(localDateTime);
    }


    /**
     * 将字符串转日期成Long类型的时间戳，格式为：yyyy-MM-dd HH:mm:ss
     */
    public static Long convertTimeToLong(String time) {
        DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime parse = LocalDateTime.parse(time, ftf);
        return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 是否周末
     *
     * @return 周末 true 周中 false
     */
    public static boolean checkWeekend(long timestamp) {
        Date date = new Date(timestamp);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int weekday = c.get(Calendar.DAY_OF_WEEK);
        return weekday == 1 || weekday == 7;
    }

    /**
     * 获取当前时刻的月
     *
     * @return 形如：yyyyMM 如：201803
     */
    public static String getNowMonth() {
        return LocalDate.now().format(YYYY_MM_FORMATTER);
    }

    /**
     * 获取当前时刻年月日
     *
     * @return 形如：yyyyMMdd 如：20170703
     */
    public static String getNowDay() {
        return LocalDate.now().format(YYYYMMDD_FORMATTER);
    }




    /**
     * 获取当前时刻的年月日小时
     *
     * @return 形如：yyyyMMddHH 如： 2018070321
     */
    public static String getNowHour() {
        return LocalDateTime.now().format(YYYYMMDDHH_FORMATTER);
    }

    /**
     * 获取指定天数前0点的毫秒值
     *
     * @param beforeDays 多少天前
     * @return 如：现在是2018-07-13 获取6天前零点的毫秒值 -> 1530892800000 （2018-07-07 00:00:00）
     */
    public static long getBeforeDaysZeroStamp(int beforeDays) {
        return LocalDateTime.now().minusDays(beforeDays).withHour(0).withMinute(0).withSecond(0)
                .toEpochSecond(ZoneOffset.of("+8")) * 1000;
    }

    /**
     * 获取指定天数前23:59:59的毫秒值
     *
     * @param beforeDays 多少天前
     * @return 如：现在是2018-07-13 获取6天前23:59:59的毫秒值 -> 1530979199000 （2018-07-07 23:59:59）
     */
    public static long getBeforeDaysLastStamp(int beforeDays) {
        return LocalDateTime.now().minusDays(beforeDays).withHour(23).withMinute(59).withSecond(59)
                .toEpochSecond(ZoneOffset.of("+8")) * 1000;
    }

    /**
     * 获取指定天数后当天的23:59:59的毫秒值
     *
     * @param afterDays 多少天前， 0 今天
     * @return 如：现在是2018-07-13 获取6天前23:59:59的毫秒值 -> 1530979199000 （2018-07-07 23:59:59）
     */
    public static long getAfterDaysLastStamp(int afterDays) {
        return LocalDateTime.now().plusDays(afterDays).withHour(23).withMinute(59).withSecond(59)
                .toEpochSecond(ZoneOffset.of("+8")) * 1000;
    }

    /**
     * 将指定时间戳格式化成yyyy-MM-dd
     *
     * @param timestamp 时间戳
     * @return 如：2018-07-03
     */
    public static String formatTimestamp(long timestamp) {
        return YYYY_MM_DD_FORMATTER
                .format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));
    }

    /**
     * 将指定时间戳格式化成yyyy年MM月dd日 时分秒
     *
     * @param timestamp 时间戳
     * @return 如：2018-07-03
     */
    public static String formatTimestampToStr(long timestamp) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return localDateTime.getYear() + "年" + localDateTime.getMonth().getValue() + "月" + localDateTime.getDayOfMonth()
                + "日" + localDateTime.getHour() + "时" + localDateTime.getMinute() + "分" + localDateTime.getSecond()
                + "秒";
    }

    public static String timestampToStr(long timestamp) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return localDateTime.getMonth().getValue() + "." + localDateTime.getDayOfMonth();
    }

    /**
     * 将时间戳格式化成指定格式
     *
     * @param timestamp 时间戳
     * @param formatter 指定格式
     * @return 返回格式化后的字符串
     */
    public static String formatTimestampWithFormatter(long timestamp, DateTimeFormatter formatter) {
        return formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));
    }


    /**
     * 获取当前日期前n天的年月日
     *
     * @param beforeDays 前几天
     * @return 如今天2018-07-13 前6天的返回 yyyyMMdd 如：20170707
     */
    public static String getBeforeDays(int beforeDays) {
        return LocalDate.now().minusDays(beforeDays).format(YYYYMMDD_FORMATTER);
    }


    /**
     * 获取当前日期前n天的月
     *
     * @param beforeDays 前几天
     * @return 如今天2018-07-13 前6天的返回  7
     */
    public static Integer getBeforeDaysMonth(int beforeDays) {
        return LocalDate.now().minusDays(beforeDays).getMonth().getValue();
    }


    /**
     * 根据long值时间戳获取月份
     *
     * @param timestamp long值时间戳
     * @return 月
     */
    public static Integer getMonth(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return calendar.get(Calendar.MONTH) + 1;
    }

    /**
     * 根据long值时间戳获取日期
     *
     * @param timestamp long值时间戳
     * @return 月
     */
    public static Integer getDay(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        //输出日
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * 根据long值时间戳获取日期
     *
     * @param timestamp long值时间戳
     * @return 月
     */
    public static Integer getHour(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        //输出小时
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 根据long值时间戳获取日期
     *
     * @param timestamp long值时间戳
     * @return 月
     */
    public static Integer getMinute(long timestamp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        //输出小时
        return calendar.get(Calendar.MINUTE);
    }

    /**
     * 获取当前日期前n天的年月日
     *
     * @param beforeDays 前几天
     * @return 如今天2018-07-13 前6天的返回 7
     */
    public static Integer getBeforeDaysDay(int beforeDays) {
        return LocalDate.now().minusDays(beforeDays).getDayOfMonth();
    }

    public static long getNowDayHourMinuteTime(int hour, int minutes) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, minutes);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();

    }

    /**
     * 获取上周周几的日期,默认一周从周一开始
     *
     * @param dayOfWeek {@link Calendar}
     * @return
     */
    public static Date getDayOfLastWeek(int dayOfWeek) {
        return getDayOfWeek(Calendar.MONDAY, dayOfWeek, -1);
    }

    /**
     * 获取上(下)周周几的日期
     * @param firstDayOfWeek {@link Calendar}
     * 值范围
     * <code>MONDAY</code>,
     * <code>TUESDAY</code>,
     * <code>WEDNESDAY</code>,
     * <code>THURSDAY</code>,
     * <code>FRIDAY</code>,
     * <code>SATURDAY</code>,
     * <code>SUNDAY</code>
     * @param dayOfWeek {@link Calendar}
     * @param weekOffset  周偏移，上周为-1，本周为0，下周为1，以此类推
     * @return
     */
    public static Date getDayOfWeek(int firstDayOfWeek, int dayOfWeek, int weekOffset) {
        if (dayOfWeek > Calendar.SATURDAY || dayOfWeek < Calendar.SUNDAY) {
            return null;
        }
        if (firstDayOfWeek > Calendar.SATURDAY || firstDayOfWeek < Calendar.SUNDAY) {
            return null;
        }
        Calendar date = Calendar.getInstance(Locale.CHINA);
        date.setFirstDayOfWeek(firstDayOfWeek);
        //周数减一，即上周
        date.add(Calendar.WEEK_OF_MONTH, weekOffset);
        //日子设为周几
        date.set(Calendar.DAY_OF_WEEK, dayOfWeek);
        //时分秒全部置0
        date.set(Calendar.HOUR_OF_DAY, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        return date.getTime();
    }

    public static String getMmDdPattern(String time) {
        DateFormat fmt =new SimpleDateFormat("yyyy-MM-dd");
//        DateFormat fmt =new SimpleDateFormat("yyyy年MM月dd日");
        Date date = null;
        try {
            date = fmt.parse(time);
        } catch (ParseException e) {
            //log.error("字符串转换为时间异常", e);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("MM月dd日");
        return sdf.format(date);

    }


    public static void main(String[] args) {
        Date monday =getDayOfLastWeek(Calendar.MONDAY);
        Date sunday = getDayOfLastWeek(Calendar.SUNDAY);
    }


}
