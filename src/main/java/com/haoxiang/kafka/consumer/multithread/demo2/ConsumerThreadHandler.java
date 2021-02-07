package com.haoxiang.kafka.consumer.multithread.demo2;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerThreadHandler.java
 * @Description consumer多线程管理类
 * 1、创建线程池
 * 2、为每个线程分配消息
 * 3、提交位移
 * @createTime 2021年01月26日 15:42:00
 */
public class ConsumerThreadHandler<K, V> {

    private final KafkaConsumer<K, V> consumer;

    private ExecutorService executors;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                offsets.clear();
            }
        });
    }

    public void consume(int threadNumber) {
        executors = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000);
                if (!records.isEmpty()) {
                    executors.submit(new ConsumerWorker<>(records, offsets));
                    commitOffsets();
                }
            }
        } catch (WakeupException e) {
            // ignore
        } finally {
            commitOffsets();
            consumer.close();
        }
    }

    /*
     * @title commitOffsets
     * @description 多线程提交offset
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-01-26 16:19:50
     * @return void
     * @throws
     */
    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> unmodifiedMap;
        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }
            unmodifiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodifiedMap);
    }

    public void close() {
        consumer.wakeup();
        executors.shutdown();
    }
}
