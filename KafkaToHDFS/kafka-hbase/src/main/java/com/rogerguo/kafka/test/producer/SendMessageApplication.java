package com.rogerguo.kafka.test.producer;

import java.util.stream.Stream;

public class SendMessageApplication {

    public static void main(String[] args) throws Exception {
        // 文件地址
        String filePath = "E:\\Projects\\idea\\kafka-test\\src\\main\\java\\com\\rogerguo\\kafka\\test\\dataset\\data.csv";
        // kafka topic
        String topic = "user-behavior";
        // kafka borker地址
        String broker = "127.0.0.1:9092";


        Stream.generate(new UserBehaviorCsvFileReader(filePath))
                .sequential()
                .forEachOrdered(new MyKafkaProducer(topic, broker));
    }
}
