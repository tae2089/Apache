package kafka.api;

import java.time.Duration;
import java.util.*;


import lombok.Data;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Data
public class Consumer <V>{
	//	private static final String TOPIC_NAME = "test";
	private String topic;
	//	private final static String SERVERS = "5.165.97.225:9092, 52.79.191.188:9092, 3.34.63.63:9092";
	private String servers;
	private KafkaConsumer<String,V> consumer;

	private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

	public Consumer(String topic,String servers){
		this.topic = topic;
		this.servers = servers;
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		this.consumer = new KafkaConsumer<>(properties);
	}


	public ArrayList<V> getMessageBySync(){
		this.consumer.subscribe(Collections.singletonList(this.topic));
		 ArrayList<V> messageList = new ArrayList<>();
		try {
				ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(100000));
				for (ConsumerRecord<String, V> record : records) {
					V message = record.value();
					logger.info("{}",message);
					messageList.add(message);
				}
				consumer.commitSync();
			return messageList;
		} catch(Exception e) {
			// exception
			logger.info(e.getMessage());
			return null;
		}
	}

	public ArrayList<V> getMessageBySyncCustom(){
		this.consumer.subscribe(Collections.singletonList(this.topic));
		ArrayList<V> messageList = new ArrayList<>();
		try {
			ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(100000));
			Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
			for (ConsumerRecord<String, V> record : records) {
				V message = record.value();
				logger.info("{}",message);
				messageList.add(message);
				currentOffset.put(new TopicPartition(record.topic(),record.partition())
													,new OffsetAndMetadata(record.offset()+1,null));
				consumer.commitSync(currentOffset);
			}
			return messageList;
		} catch(Exception e) {
			// exception
			logger.info(e.getMessage());
			return null;
		}
	}

	public ArrayList<V> getMessageByAsync(){
		this.consumer.subscribe(Collections.singletonList(this.topic));
		ArrayList<V> messageList = new ArrayList<>();
		try {
			ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(100000));
			for (ConsumerRecord<String, V> record : records) {
				V message = record.value();
				logger.info("{}",message);
				messageList.add(message);
			}
			consumer.commitAsync();
			return messageList;
		} catch(Exception e) {
			// exception
			logger.info(e.getMessage());
			return null;
		}
	}


	public ArrayList<V> getMessageByAsyncCustom(){
		this.consumer.subscribe(Collections.singletonList(this.topic));
		ArrayList<V> messageList = new ArrayList<>();
		try {
			ConsumerRecords<String, V> records = consumer.poll(Duration.ofMillis(100000));
			for (ConsumerRecord<String, V> record : records) {
				V message = record.value();
				logger.info("{}",message);
				messageList.add(message);
			}
			consumer.commitAsync(new OffsetCommitCallback() {
				@Override
				public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
					if(e != null){
						logger.error("Commit Failed");
						logger.error("Commit faild for offsets {}", offsets, e);
					} else{
						logger.info("Commit Succeeded");
					}
				}
			});
			return messageList;
		} catch(Exception e) {
			// exception
			logger.info(e.getMessage());
			return null;
		}
	}








	public void close(){
		this.consumer.close();
	}

}
