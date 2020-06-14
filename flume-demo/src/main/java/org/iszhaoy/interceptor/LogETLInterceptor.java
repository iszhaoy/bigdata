package org.iszhaoy.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

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
        // 清洗数据 ETL

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return null;
    }

    @Override
    public void close() {

    }
}
