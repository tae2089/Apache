package kafka.api;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;



@Data
public class Producer<V> {
	//	Topic name "test";
	private String topic;
	//	Severs:port = "5.165.97.225:9092, 52.79.191.188:9092, 3.34.63.63:9092";
	private String servers;
	private KafkaProducer<String,V> producer;
	//	private static final String FIN_MESSAGE = "exit";
	private static final Logger logger = LoggerFactory.getLogger(Producer.class);



	public Producer(String topic, String servers){
		this.topic = topic;
		this.servers = servers;
		Properties prop = new Properties();
		prop.put("bootstrap.servers", servers);
		prop.put("key.serializer", StringSerializer.class.getName());

		//???????? 여기 부분 어떻게 수정해야 할까?
		prop.put("value.serializer", Serializer.class.getName());
		this. producer = new KafkaProducer<>(prop);
	}


	//K - Key V- Value
	public void sendMessage(V message) {
		ProducerRecord<String, V> record = new ProducerRecord<>(this.topic, message);
		logger.info("{}", record);
		this.producer.send(record);
		this.producer.flush();
	}

	public void close(){
		this.producer.close();
	}

}
