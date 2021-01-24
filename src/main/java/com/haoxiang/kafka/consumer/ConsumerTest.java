package com.haoxiang.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerTest.java
 * @Description 各种消费者demo
 * @createTime 2021年01月22日 13:50:00
 */
public class ConsumerTest {

    private static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    private static volatile boolean isActive;

    public static void main(String[] args) {
        consumerSample();
    }

    /*
     * @title consumerSample
     * @description 消费者简单样例
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-22 19:08:59
     * @return void
     * @throws
     */
    private static void consumerSample() {
        String topic = "test-topic";
        String groupId = "test-group";
        Properties properties = new Properties();
        // 必须指定
        properties.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        // 必须指定
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        // 从最早的消息开始读取
        properties.put("auto.offset.reset", "earliest");
        // 必须指定
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 必须指定
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 创建consumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isActive) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    /*
     * @title consumerCommitAsyncNonAutoCommit
     * @description 手动提交订阅分区的offset
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-22 19:08:21
     * @return void
     * @throws
     */
    private static void consumerCommitAsyncNonAutoCommit() {
        String topic = "test-topic";
        String groupId = "test-group";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 500;
        try {
            while (isActive) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // 遍历订阅的分区
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecordList = records.records(partition);
                    // 模拟消费
                    for (ConsumerRecord<String, String> record : partitionRecordList) {
                        logger.info("offset={}, value={}", record.offset(), record.value());
                    }
                    long lastOffset = partitionRecordList.get(partitionRecordList.size() - 1).offset();
                    // 按分区提交offset， offset为下一条待读取的消息，所以为lastOffset+1
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void insertIntoDB(List<ConsumerRecords<String, String>> buffer) {
        //模拟持久化入库
    }
}
