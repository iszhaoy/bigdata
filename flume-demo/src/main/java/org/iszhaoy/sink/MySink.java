package org.iszhaoy.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    // 获取logger对象
    Logger logger = LoggerFactory.getLogger(MySink.class);

    // 1. 定义两个属性，前后缀
    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "sizhaoy");
    }

    /**
     * 1. 获取channel
     * 2、 从channel获取事务以及数据
     * 3.  发送数据
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        // 1. 定义返回值
        Status status = null;

        // 2.  获取channel
        Channel channel = getChannel();

        // 3. 从channel获取事务
        Transaction tx = channel.getTransaction();
        // 4.  开启事务
        tx.begin();
        Event event = null;
        try {
            // 5。 从channel获取数据
            event = channel.take();
            if (event != null) {
                if (logger.isInfoEnabled()) {
                    // 6 处理实践
                    String body = new String(event.getBody());
                    logger.info(prefix + body + subfix);
                }
            } else {
                // No event found, request back-off semantics from the sink runner
                status = Status.BACKOFF;
            }

            // 7.提交事务
            tx.commit();
            // 8. 成功提交，修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            // 9. 提交事务失败
            tx.rollback();
            // 10.修改状态
            status = Status.BACKOFF;
        } finally {
            // 11. 最终关闭事务
            tx.close();
        }
        // 12 返回状态信息
        return status;
    }


}
