package com.haoxiang.kafka.consumer.multithread.demo2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerWorker.java
 * @Description 消费线程包装类
 * @createTime 2021年01月26日 16:22:00
 */
public class ConsumerWorker<K, V> implements Runnable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ConsumerRecords<K, V> records;

    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public ConsumerWorker(ConsumerRecords<K, V> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
            for (ConsumerRecord<K, V> record : partitionRecords) {
                logger.info("topic={},partition={},offset={}", record.topic(), record.partition(), record.offset());
            }
            // 上报offset
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            synchronized (offsets) {
                if (!offsets.containsKey(partition)) {
                    offsets.put(partition, new OffsetAndMetadata(lastOffset));
                } else {
                    long curr = offsets.get(partition).offset();
                    if (curr <= lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
    }
}
