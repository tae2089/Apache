package test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;




public class kafkaCusumor {
	private static final String TOPIC_NAME = "test";
	private final static String SERVERS = "5.165.97.225:9092, 52.79.191.188:9092, 3.34.63.63:9092";
	private static final String FIN_MESSAGE = "exit";

	public static void main(String[] args) {
	    Properties properties = new Properties();
	    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
	    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    properties.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_NAME);

	    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
	    consumer.subscribe(Collections.singletonList(TOPIC_NAME));

	    String message = null;
	    try {
	        do {
	            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000));

	            for (ConsumerRecord<String, String> record : records) {
	                message = record.value();
	                System.out.println(message);
	            }
	        } while (!message.equals(FIN_MESSAGE));
	    } catch(Exception e) {
	        // exception
	    } finally {
	        consumer.close();
	    }
	}
}
