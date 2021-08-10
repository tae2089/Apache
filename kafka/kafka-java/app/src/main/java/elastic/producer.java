package elastic;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
public class producer {
	private final static String TOPIC = "test";
	private final static String SERVERS = "5.165.97.225:9092, 52.79.191.188:9092, 3.34.63.63:9092";
	private static final String FIN_MESSAGE = "exit";

	public static void main(String [] args){
	    Properties prop = new Properties();
	    prop.put("bootstrap.servers", SERVERS);
	    prop.put("key.serializer", StringSerializer.class.getName());
	    prop.put("value.serializer", StringSerializer.class.getName());
	    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
	    while (true){
	        Scanner sc = new Scanner(System.in);
	        System.out.println("Producing > ");
	        String message = sc.nextLine();
	        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
	        try { producer.send(record, (metadata, exception) -> {
	                    // some exception
	                }
	        );
	        }catch (Exception e) {
	            // exception
	        }
	        finally {
	            producer.flush();
	        }
	        if(FIN_MESSAGE.equals(message)) {
	            producer.close(); break;
	        }



	    }


	}

}
