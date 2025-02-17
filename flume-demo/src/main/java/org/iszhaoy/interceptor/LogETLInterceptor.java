package org.iszhaoy.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author zhaoyu
 * @date 2020/6/13
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 清洗数据 ETL {} => { xxx 脏数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        // 区分类型处理
        if (log.contains("start")) {
            //
            // 验证服务器启动日志逻辑
            if (LogUtils.validateStart(log)) {
                return event;
            }
        } else {
            // 检验时间日志逻辑
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        // 多Event处理
        List<Event> list = new ArrayList<>();

        // 取出合格的校验数据
        for (Event event : events) {
            if (event != null) {
                list.add(event);
            }
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
