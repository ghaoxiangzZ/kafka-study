package com.haoxiang.kafka.brokerapi.server.consumer;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ConsumerAPI.java
 * @Description 以API的形式管理集群-服务端API管理consumer
 * @createTime 2021年02月20日 15:31:00
 */
public class ServerConsumerAPI {

    public static void main(String[] args) {
        findConsumerGroupByAPI();
    }

    /*
     * @title findConsumerGroupByAPI
     * @description 查询消费组信息
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-02-20 15:18:56
     * @return void
     * @throws
     */
    private static void findConsumerGroupByAPI() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.188.129:9092,192.168.188.129:9093,192.168.188.129:9094");
        AdminClient client = AdminClient.create(props);
        Map<Node, List<GroupOverview>> groups = JavaConversions.mapAsJavaMap(client.listAllGroups());
        System.out.println("打印消费组信息开始-------------------------------------");
        for (Map.Entry<Node, List<GroupOverview>> entry : groups.entrySet()) {
            Iterator<GroupOverview> groupOverviewList = JavaConversions.asJavaIterator(entry.getValue().iterator());
            while (groupOverviewList.hasNext()) {
                GroupOverview overview = groupOverviewList.next();
                System.out.println("groupId=" + overview.groupId());
                Map<TopicPartition, Object> offsets = JavaConversions.mapAsJavaMap(client.listGroupOffsets(overview.groupId()));
                long offset = (long) offsets.get(new TopicPartition("test", 0));
                System.out.println("topic=test, offset=" + offset);
            }
        }
        System.out.println("打印消费组信息完毕-------------------------------------");
        client.close();
    }
}
