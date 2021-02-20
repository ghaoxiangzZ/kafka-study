package com.haoxiang.kafka.brokerapi.server.topic;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author haoxiang_guo
 * @version 1.0.0
 * @ClassName ServerAPI.java
 * @Description 以API的形式管理集群-服务端API管理topic
 * @createTime 2021年02月20日 14:58:00
 */
public class ServerTopicAPI {

    public static void main(String[] args) {
        findTopicDescriptionByAPI();
    }

    /*
     * @title createTopicByAPI
     * @description 创建topic
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-02-20 15:18:56
     * @return void
     * @throws
     */
    private static void createTopicByAPI() {
        ZkUtils zkUtils = ZkUtils.apply("192.168.188.129:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建单分区 单副本 名字为api_created_topic的topic
        AdminUtils.createTopic(zkUtils, "api_created_topic", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    /*
     * @title deleteTopicByAPI
     * @description 删除topic
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-02-20 15:19:27
     * @return void
     * @throws
     */
    private static void deleteTopicByAPI() {
        ZkUtils zkUtils = ZkUtils.apply("192.168.188.129:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 删除topic api_created_topic
        AdminUtils.deleteTopic(zkUtils, "api_created_topic");
        zkUtils.close();
    }

    /*
     * @title findTopicDescriptionByAPI
     * @description 查询API详情
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-02-20 15:22:02
     * @return void
     * @throws
     */
    private static void findTopicDescriptionByAPI() {
        ZkUtils zkUtils = ZkUtils.apply("192.168.188.129:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "api_created_topic");
        Iterator it = props.entrySet().iterator();
        System.out.println("打印topic参数开始-----------------------------------------------");
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            System.out.println("key=" + entry.getKey() + ", value=" + entry.getValue());
        }
        System.out.println("打印topic参数完毕-----------------------------------------------");
        zkUtils.close();
    }

    /*
     * @title findTopicDescriptionByAPI
     * @description 修改topic参数
     * @author haoxiang_guo
     * @param []
     * @updateTime 2021-02-20 15:22:02
     * @return void
     * @throws
     */
    private static void modifyTopicPropertiesByAPI() {
        ZkUtils zkUtils = ZkUtils.apply("192.168.188.129:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "api_created_topic");
        // 增加
        props.put("min.cleanable.dirty.ratio", "0.3");
        // 删除
        props.remove("max.message.bytes");
        // 提交修改
        AdminUtils.changeTopicConfig(zkUtils, "api_created_topic", props);
        zkUtils.close();
    }
}