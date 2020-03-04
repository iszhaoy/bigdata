package com.iszhaoy.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        // 1. 取出数据
        String value = record.value();

        // 2. 创建一个新的ProducerRecord对象，并返回

        ProducerRecord<String, String> timeRecord = new ProducerRecord<>(record.topic(), record.partition(), record.key(),
                System.currentTimeMillis() + ":" + value);
        System.out.println(timeRecord);
        return timeRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
