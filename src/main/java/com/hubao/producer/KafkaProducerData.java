package com.hubao.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaProducerData {

    public static void main(String[] args) throws IOException {
        //创建propertise参数
        Properties properties = new Properties();
        //kafka地址
        properties.put("bootstrap.servers", "192.168.20.140:9092,192.168.20.141:9093,192.168.20.142:9094");
        //properties.put("bootstrap.servers", "192.168.40.230:9092,192.168.40.231:9093,192.168.40.232:9094");
        //key.serializer: 键序列化，默认org.apache.kafka.common.serialization.StringDeserializer。
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value.deserializer:值序列化，默认org.apache.kafka.common.serialization.StringDeserializer。
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

/*        （1）acks指定必须要有多少个partition副本收到消息，生产者才会认为消息的写入是成功的。

        acks=0，生产者不需要等待服务器的响应，以网络能支持的最大速度发送消息，吞吐量高，但是如果broker没有收到消息，生产者是不知道的

        acks=1，leader partition收到消息，生产者就会收到一个来自服务器的成功响应

        acks=all，所有的partition都收到消息，生产者才会收到一个服务器的成功响应

        （2）buffer.memory，设置生产者内缓存区域的大小，生产者用它缓冲要发送到服务器的消息。

        （3）compression.type，默认情况下，消息发送时不会被压缩，该参数可以设置成snappy、gzip或lz4对发送给broker的消息进行压缩

        （4）retries，生产者从服务器收到临时性错误时，生产者重发消息的次数

        （5）batch.size，发送到同一个partition的消息会被先存储在batch中，该参数指定一个batch可以使用的内存大小，单位是byte。不一定需要等到batch被填满才能发送

        （6）linger.ms，生产者在发送消息前等待linger.ms，从而等待更多的消息加入到batch中。如果batch被填满或者linger.ms达到上限，就把batch中的消息发送出去

        （7）max.in.flight.requests.per.connection，生产者在收到服务器响应之前可以发送的消息个数*/
        //request.required.acks：指定消息是否确定已发送成功，如果不设置值，则默认为“发送或不确认‘，可能会导致数据丢失。
        properties.put("request.required.acks", "1");

        //创建消费者
        Producer<String, String> producer = new KafkaProducer<>(properties);

        producer.send(new ProducerRecord<String, String>("online_rtmap_data", "","{\"_id\":\"f43bb2d34a33499dabc778cfad2c1f67\",\"address_detail\":{\"comp_lat\":\"25.260534\",\"country\":\"中国\",\"province\":\"广西壮族自治区\",\"city\":\"桂林市\",\"province_id\":\"450000\",\"district\":\"七星区\",\"comp_lng\":\"110.330980\",\"location\":\"110.330980,25.260534\",\"area_id\":\"450305\",\"township\":[],\"city_id\":\"450300\"},\"adr_not_reg_adr\":\"0\",\"company_all\":{\"company_type\":\"\",\"company_source_id\":\"\",\"industry_id\":\"135\",\"business_term\":\"\",\"grab_update_time\":\"\",\"company_used_name\":\"\",\"company_website\":\"\",\"reg_address\":\"\",\"company_email\":\"\",\"taxpayer_qualification\":\"\",\"industry\":\"娱乐业\",\"approved_date\":\"\",\"staff_size\":\"\",\"reg_capital_type\":\"人民币\",\"organization_code\":\"\",\"company_relation\":\"\",\"contributors_in\":\"\",\"reg_organs\":\"\",\"contributed_capital\":\"\",\"share_pic\":\"\",\"company_type_old\":\"\",\"intro\":\"\",\"social_credit_code\":\"\",\"now_get_lock_id\":\"\",\"logo_image_src\":\"\",\"company_name_english\":\"\",\"legal_person\":\"俞茜兰\",\"address\":\"桂林市育才路综合楼３３－２－１\",\"taxpayer_code\":\"\",\"reg_capital_num\":\"20\",\"data_form\":\"MapInfo\",\"history\":\"\",\"company_tel\":\"13883101282\",\"business_scope\":\"\",\"reg_date\":\"2002-04-15\",\"registration_code\":\"\",\"capital_type_id\":\"1000\",\"regist_status_id\":\"2\",\"company_type_id\":\"\",\"reg_status\":\"存续\",\"company_name\":\"桂林市地球村网络管理有限公司\"},\"company_info_id\":\"006f0a020d4b4400aa84e86b2f3c7280\",\"grab_time\":\"2019-04-16 13:19:01\",\"md5_id\":\"78afe318e00cd272db68d35d3509de3d\",\"sign\":{\"start_clean_time\":1555391941028,\"clean_cost_time\":91,\"end_clean_time\":1555391941119,\"data_level\":1}}"));
/*
        ArrayList<String> strings = new ArrayList<>();

        Integer count = 0;
        BufferedReader in = null;
        BufferedWriter out = null;
        //in = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:\\工作文件\\6月\\sj_sourceid_name_0618.txt")), "utf-8"));

        //数据写出
        //out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("D:\\output\\companyname.txt")), "utf-8"));
        String line = "";
        while((line = in.readLine())!=null){
            StringBuffer buffer = new StringBuffer();
            String[] string = buffer.append(line).toString().split("\\$");
            String company_name = string[1];
            String company_source_id = string[0];

            company_source_id.replaceAll("","");

            //判断source_id是否存在
            if (company_source_id != null && !company_source_id.equals("\\N") && !company_source_id.equals("")) {
                //DealClass.sendSourceId(company_source_id);
                count++;
                *//*String key = count.toString();
                if (count > 125000) {
                    //producer.send(new ProducerRecord<String, String>("songjiang_data_01", key,company_source_id));
                    producer.send(new ProducerRecord<String, String>("online_rtmap_data", key,"{\"_id\":\"f43bb2d34a33499dabc778cfad2c1f67\",\"address_detail\":{\"comp_lat\":\"25.260534\",\"country\":\"中国\",\"province\":\"广西壮族自治区\",\"city\":\"桂林市\",\"province_id\":\"450000\",\"district\":\"七星区\",\"comp_lng\":\"110.330980\",\"location\":\"110.330980,25.260534\",\"area_id\":\"450305\",\"township\":[],\"city_id\":\"450300\"},\"adr_not_reg_adr\":\"0\",\"company_all\":{\"company_type\":\"\",\"company_source_id\":\"\",\"industry_id\":\"135\",\"business_term\":\"\",\"grab_update_time\":\"\",\"company_used_name\":\"\",\"company_website\":\"\",\"reg_address\":\"\",\"company_email\":\"\",\"taxpayer_qualification\":\"\",\"industry\":\"娱乐业\",\"approved_date\":\"\",\"staff_size\":\"\",\"reg_capital_type\":\"人民币\",\"organization_code\":\"\",\"company_relation\":\"\",\"contributors_in\":\"\",\"reg_organs\":\"\",\"contributed_capital\":\"\",\"share_pic\":\"\",\"company_type_old\":\"\",\"intro\":\"\",\"social_credit_code\":\"\",\"now_get_lock_id\":\"\",\"logo_image_src\":\"\",\"company_name_english\":\"\",\"legal_person\":\"俞茜兰\",\"address\":\"桂林市育才路综合楼３３－２－１\",\"taxpayer_code\":\"\",\"reg_capital_num\":\"20\",\"data_form\":\"MapInfo\",\"history\":\"\",\"company_tel\":\"13883101282\",\"business_scope\":\"\",\"reg_date\":\"2002-04-15\",\"registration_code\":\"\",\"capital_type_id\":\"1000\",\"regist_status_id\":\"2\",\"company_type_id\":\"\",\"reg_status\":\"存续\",\"company_name\":\"桂林市地球村网络管理有限公司\"},\"company_info_id\":\"006f0a020d4b4400aa84e86b2f3c7280\",\"grab_time\":\"2019-04-16 13:19:01\",\"md5_id\":\"78afe318e00cd272db68d35d3509de3d\",\"sign\":{\"start_clean_time\":1555391941028,\"clean_cost_time\":91,\"end_clean_time\":1555391941119,\"data_level\":1}}"));
                }*//*
                producer.send(new ProducerRecord<String, String>("online_rtmap_data", key,"{\"_id\":\"f43bb2d34a33499dabc778cfad2c1f67\",\"address_detail\":{\"comp_lat\":\"25.260534\",\"country\":\"中国\",\"province\":\"广西壮族自治区\",\"city\":\"桂林市\",\"province_id\":\"450000\",\"district\":\"七星区\",\"comp_lng\":\"110.330980\",\"location\":\"110.330980,25.260534\",\"area_id\":\"450305\",\"township\":[],\"city_id\":\"450300\"},\"adr_not_reg_adr\":\"0\",\"company_all\":{\"company_type\":\"\",\"company_source_id\":\"\",\"industry_id\":\"135\",\"business_term\":\"\",\"grab_update_time\":\"\",\"company_used_name\":\"\",\"company_website\":\"\",\"reg_address\":\"\",\"company_email\":\"\",\"taxpayer_qualification\":\"\",\"industry\":\"娱乐业\",\"approved_date\":\"\",\"staff_size\":\"\",\"reg_capital_type\":\"人民币\",\"organization_code\":\"\",\"company_relation\":\"\",\"contributors_in\":\"\",\"reg_organs\":\"\",\"contributed_capital\":\"\",\"share_pic\":\"\",\"company_type_old\":\"\",\"intro\":\"\",\"social_credit_code\":\"\",\"now_get_lock_id\":\"\",\"logo_image_src\":\"\",\"company_name_english\":\"\",\"legal_person\":\"俞茜兰\",\"address\":\"桂林市育才路综合楼３３－２－１\",\"taxpayer_code\":\"\",\"reg_capital_num\":\"20\",\"data_form\":\"MapInfo\",\"history\":\"\",\"company_tel\":\"13883101282\",\"business_scope\":\"\",\"reg_date\":\"2002-04-15\",\"registration_code\":\"\",\"capital_type_id\":\"1000\",\"regist_status_id\":\"2\",\"company_type_id\":\"\",\"reg_status\":\"存续\",\"company_name\":\"桂林市地球村网络管理有限公司\"},\"company_info_id\":\"006f0a020d4b4400aa84e86b2f3c7280\",\"grab_time\":\"2019-04-16 13:19:01\",\"md5_id\":\"78afe318e00cd272db68d35d3509de3d\",\"sign\":{\"start_clean_time\":1555391941028,\"clean_cost_time\":91,\"end_clean_time\":1555391941119,\"data_level\":1}}"));
                //System.out.println("key:"+key+",company_source_id:"+company_source_id);
            } else {
                //使用company_name更新
                //先写出到文件中
                out.write(company_name+"\n");
            }

        }*/
/*
        for (int i = 0 ; i < strings.size() ; i++ ) {
            producer.send(new ProducerRecord<String, String>("my-topic05", "1", strings.get(i)));
        }
*/


        producer.close();


    }



}
