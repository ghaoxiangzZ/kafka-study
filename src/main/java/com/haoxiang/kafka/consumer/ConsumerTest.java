package com.haoxiang.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerTest.java
 * @Description 各种消费者demo
 * @createTime 2021年01月22日 13:50:00
 */
public class ConsumerTest {

    private static Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    private static volatile boolean isActive = Boolean.TRUE;

    public static void main(String[] args) {
        consumerSaveOffsetToDB();
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
        properties.put("key.deserializer", StringDeserializer.class.getName());
        // 必须指定
        properties.put("value.deserializer", StringDeserializer.class.getName());
        // 创建consumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (isActive) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
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
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 500;
        try {
            while (isActive) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
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

    /*
     * @title consumerSaveOffsetToDB
     * @description 外部存储consumer offset
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-22 19:08:21
     * @return void
     * @throws
     */
    private static void consumerSaveOffsetToDB() {
        String topic = "test-topic";
        String groupId = "test-group";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        final AtomicLong totalRebalanceTime = new AtomicLong(0L);
        final AtomicLong joinStart = new AtomicLong(0L);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    saveOffsetToDB(consumer.position(partition));
                    joinStart.set(System.currentTimeMillis());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 更新总rebalance时长
                totalRebalanceTime.addAndGet(System.currentTimeMillis() - joinStart.get());
                for (TopicPartition partition : partitions) {
                    // 读取offset
                    consumer.seek(partition, readOffsetFromDB(partition));
                }
            }
        });
        try {
            while (isActive) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic={}, partition={}, offset={}", record.topic(), record.partition(), record.offset());
                }
            }
        } finally {
            logger.info("total rebalance time={}", totalRebalanceTime.get());
            consumer.close();
        }
    }

    /*
     * @title readOffsetFromDB
     * @description 模拟从DB读取offset
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-25 23:19:57
     * @return void
     * @throws
     */
    private static long readOffsetFromDB(TopicPartition partition) {
        logger.info("read offset，partition={}", partition);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new Random().nextInt(200000);
    }

    /*
     * @title saveOffsetToDB
     * @description 模拟offset保存到DB
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-25 23:19:57
     * @return void
     * @throws
     */
    private static void saveOffsetToDB(long offset) {
        logger.info("save offset={}", offset);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
