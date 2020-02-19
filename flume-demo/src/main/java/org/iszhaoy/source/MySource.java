package org.iszhaoy.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory
            .getLogger(LoggerSink.class);
    // 定义全局的前缀
    private String prefix;
    // 定义全局的后缀
    private String subfix;

    @Override
    public void configure(Context context) {
        // 读取配置信息 全后缀复制
        prefix = context.getString("prefix");
        subfix =  context.getString("subfix","iszhaoy");
    }

    /**
     * 1. 接受数据（for循环造数据）
     * 2. 封装为事件
     * 3. 将时间发送为channel
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        // 自定义soruce如果是文件或者io流，可能会出现异常导致os file == null 又因为process是循环调用的，所以可以在开始的时候每次都校验一下全局变量，如果为null，就重新初始化
        Status status = null;

        try {
            for (int i = 0; i < 5; i++) {
                 // 2. 构造事件对象
                SimpleEvent event = new SimpleEvent();

                // 给实践设置body
                event.setBody((prefix + "--" +i + "--" +subfix).getBytes());
                // 发送给channel，自定义source不需要事务，因为 processEvent已经帮忙做了
                getChannelProcessor().processEvent(event);
                status = Status.READY;
            }
        } catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }
        // 测试用
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 返回结果
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
