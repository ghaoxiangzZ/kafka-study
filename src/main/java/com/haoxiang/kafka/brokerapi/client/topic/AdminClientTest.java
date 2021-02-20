package com.haoxiang.kafka.brokerapi.client.topic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName AdminClientTest.java
 * @Description 以API的形式管理集群-客户端API管理topic
 * @createTime 2021年02月20日 16:23:00
 */
public class AdminClientTest {

    private static final Logger logger = LoggerFactory.getLogger(AdminClientTest.class);

    private static final String TEST_TOPIC = "api-create-topic-test";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.188.129:9092");
        try (AdminClient client = AdminClient.create(props)) {
            // 描述集群信息
            describeCluster(client);
            // 创建topic
            createTopic(client);
            // 查询集群topic
            listAllTopics(client);
            // 查询topic信息
            describeTopics(client);
            // 修改topic参数配置信息
            alterConfigs(client);
            // 查询所有配置信息
            describeConfig(client);
            // 删除topic
            //deleteTopics(client);
        }
    }

    /*
     * @title describeCluster
     * @description 获取kafka集群信息
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 18:19:20
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void describeCluster(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeClusterResult result = client.describeCluster();
        logger.info("Cluster id={}, controller={}", result.clusterId().get(), result.controller().get());
        logger.info("Current Cluster nodes info:");
        for (Node node : result.nodes().get()) {
            logger.info("node info={}", node);
        }
    }

    /*
     * @title createTopic
     * @description 创建topic（异步future模式）
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 18:39:15
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void createTopic(AdminClient client) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(TEST_TOPIC, 3, (short) 3);
        CreateTopicsResult result = client.createTopics(Arrays.asList(newTopic));
        result.all().get();
    }

    /*
     * @title listAllTopics
     * @description 获取topic列表
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 22:57:24
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void listAllTopics(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult topics = client.listTopics(options);
        Set<String> topicNames = topics.names().get();
        logger.info("Current topics in this cluster:{}", topicNames);
    }

    /*
     * @title describeTopics
     * @description 获取topic详细数据 包括leader ISR等分区数据
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 23:00:20
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void describeTopics(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = client.describeTopics(Arrays.asList(TEST_TOPIC, "__consumer_offsets"));
        Map<String, TopicDescription> topics = result.all().get();
        for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
            logger.info("describeTopics key={}, value={}", entry.getKey(), entry.getValue());
        }
    }

    /*
     * @title alterConfigs
     * @description 修改topic级别参数
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 23:04:17
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void alterConfigs(AdminClient client) throws ExecutionException, InterruptedException {
        Config topicConfig = new Config(Arrays.asList(new ConfigEntry("cleanup.policy", "compact")));
        client.alterConfigs(Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC), topicConfig)).all().get();
    }

    /*
     * @title describeConfig
     * @description 获取topic参数配置
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 23:10:08
     * @return void
     * @throws
     */
    private static void describeConfig(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC)));
        Map<ConfigResource, Config> configs = result.all().get();
        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            logger.info("describeConfig key = {}, value = {}", entry.getKey(), entry.getValue());
            Collection<ConfigEntry> configEntries = entry.getValue().entries();
            for (ConfigEntry configEntry : configEntries) {
                logger.info("describeConfig ConfigEntry key = {}, value = {}", configEntry.name(), configEntry.value());
            }
        }
    }

    /*
     * @title describeConfig
     * @description 删除指定topic
     * @author haoxiang
     * @param [client]
     * @updateTime 2021-02-20 23:05:27
     * @return void
     * @throws ExecutionException, InterruptedException
     */
    private static void deleteTopics(AdminClient client) throws ExecutionException, InterruptedException {
        KafkaFuture<Void> futures = client.deleteTopics(Arrays.asList(TEST_TOPIC)).all();
        futures.get();
    }
}
