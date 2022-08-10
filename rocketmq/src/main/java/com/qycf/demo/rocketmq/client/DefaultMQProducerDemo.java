package com.qycf.demo.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class DefaultMQProducerDemo {

    static DefaultMQProducer defaultMQProducer;

    static {

        defaultMQProducer = new DefaultMQProducer("demo-producer-group");

        defaultMQProducer.setNamesrvAddr("10.66.149.90:9876;10.66.149.91:9876");

    }

    public static void produceMessage() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        defaultMQProducer.start();
        //发送单条消息
        Message msg = new Message("qycf-rocketmq-demo", "hello rocketmq".getBytes());
        SendResult sendResult = null;
        sendResult = defaultMQProducer.send(msg);
        //　输出结果
        System.out.printf("%s%n", sendResult);
        //　发送带 Key 的消息
        msg = new Message("qycf-rocketmq-demo", null, "ODS2020072615490001", "{\"id\":1, \"orderNo\":\"ODS2020072615490001\",\"buyerId\":1,\"sellerId\":1  }".getBytes());
        sendResult = defaultMQProducer.send(msg);
        //　输出结果
        System.out.printf("%s%n", sendResult);
        //　批量发送
        List<Message> msgs = new ArrayList<>();
        msgs.add( new Message("qycf-rocketmq-demo", null, "ODS2020072615490002", "{\"id\":2, \"orderNo\":\"ODS2020072615490002\",\"buyerId\":1,\"sellerId\":3  }".getBytes()) );
        msgs.add( new Message("qycf-rocketmq-demo", null, "ODS2020072615490003", "{\"id\":4, \"orderNo\":\"ODS2020072615490003\",\"buyerId\":2,\"sellerId\":4  }".getBytes()) );
        sendResult = defaultMQProducer.send(msgs);
        System.out.printf("%s%n", sendResult);
        //　使用完毕后，关闭消息发送者
        defaultMQProducer.shutdown();
    }

}
