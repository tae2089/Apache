package com.study.producer;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;
import java.util.Scanner;

public class Producer {
    private static final String TOPIC_NAME = "taeng";
    private static final String FIN_MESSAGE = "exit";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put("bootstrap.servers", "localhost:9092"); // kafka host 및 server 설정
        configs.put("acks", "all");                         // 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않습니다.
        configs.put("block.on.buffer.full", "true");        // 서버로 보낼 레코드를 버퍼링 할 때 사용할 수 있는 전체 메모리의 바이트수
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   // serialize 설정
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // serialize 설정

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        // message 전달
        for (int i = 0; i < 5; i++) {
            String v = "hello"+i;
            producer.send(new ProducerRecord<String, String>("test", v));
        }

        // 종료
        producer.flush();
        producer.close();
    }
}
