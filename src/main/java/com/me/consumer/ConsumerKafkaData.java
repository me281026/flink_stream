package com.me.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.Collections;
import java.util.Properties;

/**
 * @author hubao
 *
 */

public class ConsumerKafkaData {

    /**
     * 消费Kafka数据
     * @param args
     * @throws IOException
     */

    public static void main(String[] args) throws IOException {
        //创建propertise参数
        Properties properties = new Properties();
        //kafka地址
        properties.put("bootstrap.servers", "192.168.20.140:9092,192.168.20.141:9092,192.168.20.142:9092");
        //properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

        //group.id：组名 不同组名可以重复消费。例如你先使用了组名A消费了kafka的1000条数据，
        //但是你还想再次进行消费这1000条数据，并且不想重新去产生，那么这里你只需要更改组名就可以重复消费了。
        //properties.put("group.id", "flink_task");clean_data,map_test,address_book
        properties.put("group.id","address_book_test");
        //enable.auto.commit：是否自动提交，默认为true
        properties.put("enable.auto.commit", "true");
        //auto.commit.interval.ms: 从poll(拉)的回话处理时长。
        properties.put("auto.commit.interval.ms", "1000");
        //session.timeout.ms:超时时间。
        properties.put("session.timeout.ms", "6000");
        properties.put("heartbeat_interval_ms", "2000");
        //max.poll.records:一次最大拉取的条数。
        //auto.offset.reset：消费规则，默认earliest 。
        //earliest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 。
        //latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 。
        //none: topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常。
        properties.put("auto.offset.reset", "earliest");
        //key.serializer: 键序列化，默认org.apache.kafka.common.serialization.StringDeserializer。
        properties.put("key.deserializer", StringDeserializer.class.getName());
        //value.deserializer:值序列化，默认org.apache.kafka.common.serialization.StringDeserializer。
        properties.put("value.deserializer", StringDeserializer.class.getName());

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //订阅topic
        //consumer.subscribe(Collections.singletonList("online_rtmap_data"));
        //consumer.subscribe(Collections.singletonList("online_rtclean_data"));
        //consumer.subscribe(Collections.singletonList("online_rt_data"));
        //consumer.subscribe(Collections.singletonList("online_address_book"));
        consumer.subscribe(Collections.singletonList("online_address_book"));

        //3.轮询
        //消息轮询是消费者的核心API，通过一个简单的轮询向服务器请求数据，一旦消费者订阅了Topic，轮询就会处理所欲的细节，包括群组协调、partition再均衡、发送心跳
        //以及获取数据，开发者只要处理从partition返回的数据即可。
        System.out.println("---------开始消费---------");
        int num = 0;
        //写入部分数据到文件
        //数据写出
        BufferedWriter out = null;
        //标记跳出多重循环
        flag:
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\output\\data.txt")), "utf-8"));
            while (true) {//消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
                //在100ms内等待Kafka的broker返回数据.超市参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    //打出部分数据
                    //System.out.println(record.topic() + record.partition() + record.offset() + record.key() + record.value());
                    num++;
                    //System.out.println("num"+num+"=======receive: key = " + record.key() + ", value = " + record.value()+" offset==="+record.offset());
                    System.out.println(record.value());
                    if (record.value().contains("上海虎宝网络")) {
                        break flag;
                    }
                    System.out.println(record.offset()+"    "+num);
                    /*if(num == 300) {
                        break flag;
                    }*/
                    out.write(record.value()+"\n");
                }
            }

        } catch (Exception e) {
            e.getStackTrace();
        } finally {
            //退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均衡
            consumer.close();
            out.close();

        }



    }
}
