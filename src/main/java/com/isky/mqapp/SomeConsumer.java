package com.isky.mqapp;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class SomeConsumer {
    public static void main(String[] args)throws MQClientException {
        //定义一个pull消费者
        //DefaultLitePullConsumer consumer1 = new DefaultLitePullConsumer("cg");
        //定义一个push 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pg");
        consumer.setNamesrvAddr("8.222.158.188:9876");
        //从哪里开始消费,指定从第一条开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //指定消费topic与tag
        consumer.subscribe("TopicA","*");
        //指定费用广播模式  进行消费，默认为集群模式的
        //consumer.setMessageModel(MessageModel.BROADCASTING);
        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                //逐条消费消息
                for (MessageExt msg : msgs ) {
                    System.out.println(msg);
                }
                //返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启消费者进行消费
        consumer.start();
        System.out.println("消费者开始了----");
    }
}

