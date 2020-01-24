package webserver.loganalyzer.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.time.*;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.PrintWriter;
import java.nio.file.*;
import java.time.format.*;


public class ConsumerWebSrv {
	private final static String apacheLogTopic = "website-access";
	private final static String bootstrapServers = "localhost:9092";
	private final static String clientID = "WebSrvLogConsumer";
	private final static String groupID = "WebSrvLogConsumer";
	
	private final Map<String,Integer> ipCount = new HashMap<>();
	private Instant windowStart;
	private final int windowSeconds = 60;

	public void createTopic() {
		// Setup the Topic
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
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
	
	public void ConsumerLogMessages() {
		
		// Setup the Consumer
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientID);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		
		// Subscribe to the Kafka topic
		consumer.subscribe(Collections.singletonList(apacheLogTopic));
		
		try {
			while (true) {				
				ConsumerRecords<String, String> logRecords = consumer.poll(Duration.ofMillis(1000));
				
				if (logRecords == null) {
					continue;
				}
				
				
				for (ConsumerRecord<String, String> logRecord : logRecords) {			
					String logLine = logRecord.value();
					Optional<String> sourceIp = findIpAddress(logLine);
					
					Optional<Instant> timestamp = findTimestamp(logLine);
					
					if (!sourceIp.isPresent() || !timestamp.isPresent()) {
						// IP address or timestamp were not found, continue to the next line in the log
						continue;
					}
					
					if (windowStart == null) {
						// Start a new window for the timer to run the log analyzer
						windowStart = timestamp.get();
						System.out.println("New Window: " + windowStart);

						startTimer();

					}
					
					long timeElapsed = Duration.between(windowStart, timestamp.get()).getSeconds();
					if (timeElapsed > windowSeconds) {
						detectBotIps();
					}

					
					ipCount.merge(sourceIp.get(), 1, Integer::sum);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
	
	
	private Timer timer;
	
	
	public void startTimer() {

        if (timer != null) {
            timer.cancel();
        }
        
		timer = new Timer(true);
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				// Detect Botnet IP Addresses
				try {
					detectBotIps();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, windowSeconds * 1000);
		
	}
	

	private final static Path outputDir = Paths.get("/Users/sashagerner/Documents/kafka/apache-streaming/output");

	
    public synchronized void detectBotIps() throws IOException {
        if (!ipCount.isEmpty()) {
            timer.cancel();
	
            // Create output directory            
    		Files.createDirectories(outputDir);
        
    		double threshold_60s = calculateThreshold(ipCount);
    		
            Path outputFile = outputDir.resolve((windowStart.toString() + ".txt"));
	
            try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile))) {
            	ipCount.entrySet().stream()
            	.filter(entry -> entry.getValue() > threshold_60s)
            	.map(Map.Entry::getKey)
            	.forEach(writer::println);
            	
            } catch (IOException e) {
            	throw new IOException("Failed to write to a file: " + outputFile, e);
            }
	
            ipCount.clear();
        }
        
        windowStart = null;
    }

	
    private double calculateThreshold(Map<String, Integer> ipCount) {
    	
		System.out.println("Calculating threshold ");

		int stdevFactor = 2;
		
        IntSummaryStatistics stats = ipCount.values().stream()
                .mapToInt(Integer::intValue)
                .summaryStatistics();
        long count = stats.getCount();
        long sum = stats.getSum();
        double avg = stats.getAverage();
        int max = stats.getMax();

        double variance = ipCount.values().stream()
                .mapToInt(Integer::intValue)
                .mapToDouble(v -> v - avg)
                .map(v -> Math.pow(v, 2))
                .average().orElse(0d);

        double stdDev = Math.sqrt(variance);
        
        double threshold_gaussian = avg + (stdevFactor * stdDev);
        
        System.out.println("Total log messages in window: " + sum);
        System.out.println("Unique IP addresses found: "+ count);
        System.out.println("Average requests per IP: " + avg);
        System.out.println("Max requests for an IP: " + max);
        System.out.println("Variance in requests per IP: " + variance);
        System.out.println("Standard deviation in requests per IP: " + stdDev);
        
        
		System.out.println("Threshold using Standard Deviation: " + threshold_gaussian);

		double[] ipCountValues = ipCount.values().stream()
                .mapToDouble(Integer::intValue)
                .toArray();
		
		Arrays.sort(ipCountValues);
	    DescriptiveStatistics ipCountValuesStats = new DescriptiveStatistics(ipCountValues);
		double iqr = ipCountValuesStats.getPercentile(75) - ipCountValuesStats.getPercentile(25);
		
		double threshold_iqr = ipCountValuesStats.getPercentile(75) + 1.5 * iqr;
		
		System.out.println("Threshold using IQR: " + threshold_iqr);
		
		return threshold_iqr;
    }
	

    
	private final static String ipRegex = "\\b(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\b";
	private final static Pattern ipPattern = Pattern.compile(ipRegex);
	
	public Optional<String> findIpAddress(String logLine) {
		Matcher matcher = ipPattern.matcher(logLine);
		
		while (matcher.find()) {
			String potentialIp = matcher.group(0);
			return Optional.of(potentialIp);
		}
		return Optional.empty();
	}
	
    private static final String dateRegex = "\\[(\\d\\d/[A-Za-z]+?/\\d{4}:\\d\\d:\\d\\d:\\d\\d \\+\\d\\d\\d\\d)\\]";
    private static final Pattern datePattern = Pattern.compile(dateRegex);

	private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MMMM/yyyy:HH:mm:ss ZZZ");
	
	private Optional<Instant> findTimestamp(String logLine) {
		Matcher matcher = datePattern.matcher(logLine);
		
		while (matcher.find()) {
			String dateInLog = matcher.group(1);
			
			try {
				ZonedDateTime dateTime = ZonedDateTime.parse(dateInLog, dateFormatter);
				return Optional.of(dateTime.toInstant());
			} catch (DateTimeParseException e) {
				// Could not parse the date in the log
			}
		}
		return Optional.empty();
	}
	
	
	public static void main(String[] args) throws Exception{  
		
		ConsumerWebSrv consumerMessages = new ConsumerWebSrv();
		consumerMessages.createTopic();
		consumerMessages.ConsumerLogMessages();
		
	}
}
