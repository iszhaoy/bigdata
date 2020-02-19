package org.iszhaoy.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    // 声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        // 初始化
        this.addHeaderEvents = new ArrayList<>();
    }

    // 单个事件拦截
    @Override
    public Event intercept(Event event) {
        // 1. 获取时间中的头信息
        Map<String, String> headers = event.getHeaders();

        // 2. 获取时间中的body信息
        String body = new String(event.getBody());

        // 3。 根据body当中是否有hello添加不同的header
        if (body.contains("hello")) {
            // 4. 添加头信息
            headers.put("type", "hashello");
        } else {
            // 4. 添加头信息
            headers.put("type", "nohello");
        }
        // 5.把事件返回
        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {
        
        // 1.清空集合（因为使用的全局的一个变量，第二次还会有第一次的）
        addHeaderEvents.clear();
        
        // 2.遍历events ,
        for (Event event : events) {
            // 3.给每个时间添加头信息
            addHeaderEvents.add(intercept(event));
        }

        // 4. 返回结果
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    // 是一个拦截器内的静态内部类
    // a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.HostInterceptor$Builder
    public static class Builder implements Interceptor.Builder {

        // 构造器私有，使用静态内部类的build方法创建拦截器对象
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
