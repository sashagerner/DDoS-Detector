package webserver.loganalyzer.kafka;


import java.io.BufferedReader;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.io.FileReader;
import java.io.IOException;
//import java.io.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;


public class ProducerWebSrv {
	private final static String apacheLogTopic = "website-access";
	private final static String bootstrapServers = "localhost:9092";

	private final static String clientID = "WebSrvLogProducer";
	private final static String logFileName = "/Users/sashagerner/Documents/kafka/apache-streaming/apache-access-log";
	
	public void createTopic() {
		// Setup the Topic
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		final short replicationFactor = 1;
		final short partitions = 1;
				
		// Create admin client
		try (final AdminClient adminClient = AdminClient.create(props)) {
			try {
				// Define topic
				final NewTopic newTopic = new NewTopic(apacheLogTopic, partitions, replicationFactor);
				
				// Create topic
				CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
				
				// Wait for topic to get created (needs to be done as the call to create a topic is async)
				result.values().get(apacheLogTopic).get();
				
			} catch (ExecutionException e) {
				if (!(e.getCause() instanceof TopicExistsException)) {
					throw new RuntimeException(e.getMessage(), e);
				}
				// If TopicExistsException, continue running, topic already exists
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public void PublishLogMessages() {
		
		// Setup the Producer
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		// Read each line from the web server log file and send it to the Kafka topic using the producer
		try(BufferedReader reader = new BufferedReader(new FileReader(logFileName))) {
			String line = reader.readLine();
			
			while (line != null) {
				producer.send(new ProducerRecord<String,String>(apacheLogTopic, null, line));
				line = reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		producer.close();
	}
	
	public static void main(String[] args) throws Exception{
		
		ProducerWebSrv producerMessages = new ProducerWebSrv();
		producerMessages.createTopic();
		producerMessages.PublishLogMessages();
		
	}
}
	

	
	
	




