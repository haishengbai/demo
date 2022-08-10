package com.qycf.demo.rocketmq.tools;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class DefaultMQAdminExtDemo {

    static DefaultMQAdminExt defaultMQAdminExt;

    private static final AtomicLong adminIndex = new AtomicLong(0);

    static {
        final String accessKey = "rocketmq2";
        final String secretKey = "12345678";

        RPCHook rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        defaultMQAdminExt = new DefaultMQAdminExt(rpcHook, 5000);
        defaultMQAdminExt.setAdminExtGroup(defaultMQAdminExt.getAdminExtGroup() + "_" + adminIndex.getAndIncrement());
        defaultMQAdminExt.setVipChannelEnabled(Boolean.TRUE);
        defaultMQAdminExt.setUseTLS(Boolean.FALSE);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr("10.66.149.90:9876;10.66.149.90:9876");
        // todo what is instanceName use for
//        defaultMQAdminExt.setInstanceName(String.valueOf(System.currentTimeMillis()));
        defaultMQAdminExt.setUnitName("instanceUnitB");

    }

    public static void main(String[] args) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        // add topic
        addTopic();

        // delete topic
//        deleteTopic("qycf-rocketmq-demo", "DefaultCluster");


    }

    public static void deleteTopic(String topic, String clusterName) {
        try {
            defaultMQAdminExt.start();

            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            log.info("masterSet = {}", new Gson().toJson(masterSet));
            defaultMQAdminExt.deleteTopicInBroker(masterSet, topic);
            Set<String> nameServerSet = null;
            if (StringUtils.isNotBlank("10.66.149.90:9876")) {
                String[] ns ="10.66.149.90:9876".split(";");
                nameServerSet = new HashSet<String>(Arrays.asList(ns));
            }
            defaultMQAdminExt.deleteTopicInNameServer(nameServerSet, topic);
        } catch (Exception err) {
            throw Throwables.propagate(err);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    public static void addTopic() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        defaultMQAdminExt.start();

        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
        Set<String> brokerNames = changeToBrokerNameSet(clusterInfo.getClusterAddrTable(), Lists.newArrayList("DefaultCluster"), Lists.newArrayList("broker-a"));

        log.info("brokerNames = {}", new Gson().toJson(brokerNames));
        for (String brokerName : brokerNames) {
            String brokerAddr = clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr();
            log.info("broker address {}", brokerAddr);
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setTopicName("qycf-rocketmq-demo");
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            topicConfig.setPerm(6);

            defaultMQAdminExt.createAndUpdateTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(),
                    topicConfig);
        }


        /**
         *
         * */
        defaultMQAdminExt.shutdown();

    }



    protected static final Set<String> changeToBrokerNameSet(HashMap<String, Set<String>> clusterAddrTable,
                                                             List<String> clusterNameList, List<String> brokerNameList) {
        Set<String> finalBrokerNameList = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(clusterNameList)) {
            try {
                for (String clusterName : clusterNameList) {
                    finalBrokerNameList.addAll(clusterAddrTable.get(clusterName));
                }
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        if (CollectionUtils.isNotEmpty(brokerNameList)) {
            finalBrokerNameList.addAll(brokerNameList);
        }
        return finalBrokerNameList;
    }
}
