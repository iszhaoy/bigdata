package org.iszhaoy.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // 区分类型
        // 区分start类型和event类型
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 判断事件的头部，然后设置成不同的主题
        Map<String, String> headers = event.getHeaders();
        if (log.contains("start")) {
            headers.put("topic", "topic_start");
        } else {
            headers.put("topic", "topic_event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> list = new ArrayList<>();

        for (Event event : events) {
            Event intercept = intercept(event);
            list.add(intercept);
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
