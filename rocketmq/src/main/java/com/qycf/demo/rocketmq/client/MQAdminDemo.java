package com.qycf.demo.rocketmq.client;

import org.apache.rocketmq.client.MQAdmin;

public class MQAdminDemo {

    /**
     * MQ基本的管理接口, 提供对MQ基础管理功能
     *
     * */
    MQAdmin mqAdmin;
    /**
     * String key：根据 key 查找 Broker，即新主题创建在哪些 Broker 上
     * String newTopic：主题名称
     * int queueNum：主题队列个数
     * int topicSysFlag：主题的系统参数
     * */
    private void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) {
        ;
    }

}
