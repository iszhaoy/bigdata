package table.function;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EavlFormatDateToLong extends ScalarFunction implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(EavlFormatDateToLong.class);

    private static final String format = "yyyy-MM-dd HH:mm:ss";
    private  DateTimeFormatter df = DateTimeFormatter.ofPattern(format);
    private SimpleDateFormat sdf = new SimpleDateFormat(format);

    public long eval(String date) {
        try {
            return LocalDateTime.parse(date, df).toInstant(ZoneOffset.of("+8")).toEpochMilli() / 1000L;
            //return sdf.parse(date).getTime() / 1000L;
        } catch (Exception e) {
            log.error("解析日期错误: " + date);
        }
        return 0;
    }
}
