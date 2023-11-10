package com.isky.mqapp;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
//同步发行消息
public class SyncProducer {
    public static void main(String[] args) throws Exception{
        //创建一个生产者producer,参数为创建一个生产者producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameserver地址
        producer.setNamesrvAddr("8.222.158.188:9876");
        //设置当前发送失败后的重试次数，默认不设置是2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时实现为5秒，默认是3秒
        producer.setSendMsgTimeout(5000);
        //开启生产者
        producer.start();
        //生产并发送100条消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("someTopic", "someTag", body);
            //为消息指定key
            msg.setKeys("key-" + i );
            //发送消息
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        }
        //关闭producer
        producer.shutdown();
    }
}

