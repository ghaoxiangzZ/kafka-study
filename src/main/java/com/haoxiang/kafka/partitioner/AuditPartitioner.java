package com.haoxiang.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName AuditPartitioner.java
 * @Description 自定义kafka分区策略
 * @createTime 2021年01月19日 19:41:00
 */
public class AuditPartitioner implements Partitioner {

    private Random random;

    @Override
    public void configure(Map<String, ?> map) {
        random = new Random();
    }

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String key = (String) o1;
        List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(s);
        int partitionCnt = partitionInfoList.size();
        int auditPartition = partitionCnt - 1;
        return key == null || key.isEmpty() || !key.contains("audit")
                ? random.nextInt(partitionCnt - 1) : auditPartition;
    }

    @Override
    public void close() {

    }


}
