package com.iszhaoy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class CustomConsumerCotumserOffset {

    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {

        //创建配置信息
        Properties props = new Properties();

        //Kafka集群
        props.put("bootstrap.servers", "hadoop01:9092");
        //消费者组，只要group.id相同，就属于同一个消费者组
        props.put("group.id", "customconsumertest");

        //关闭自动提交offset
        props.put("enable.auto.commit", "false");

        //Key和Value的反序列化类
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new
                KafkaConsumer<>(props);

        //消费者订阅主题
        consumer.subscribe(Arrays.asList("customconsumertest"), new ConsumerRebalanceListener() {

            //该方法会在Rebalance之前调用
            @Override
            public void
            onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Rebalance之前调用");
                commitOffset(currentOffset);
            }

            //该方法会在Rebalance之后调用
            @Override
            public void
            onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Rebalance之后调用");
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));// 定位到最近提交的offset位置继续消费
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d offset = %d, key = %s, value = %s \n",
                        record.partition(), record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            commitOffset(currentOffset);//异步提交
        }
    }

    //获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
