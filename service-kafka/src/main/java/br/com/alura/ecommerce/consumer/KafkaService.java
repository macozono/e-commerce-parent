package br.com.alura.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction<T> parse;
	private final String groupId;

	private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> of) {
		this.groupId = groupId;
		this.parse = parse;
		this.consumer = new KafkaConsumer<>(properties(of));
	}
	
	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse) {
		this(parse, groupId, Map.of());
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type) {
		this(parse, groupId, Map.of());
		consumer.subscribe(Collections.singletonList(topic));
	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type) {
		this(parse, groupId, Map.of());
		consumer.subscribe(topic);
	}

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> of) {
		this(parse, groupId, of);
		consumer.subscribe(topic);
	}
	
	public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse) {
		this(parse, groupId, Map.of());
		consumer.subscribe(topic);
	}

	@SuppressWarnings({ "unchecked", "resource", "rawtypes" })
	public void run() throws ExecutionException, InterruptedException {
		try (var deadLetter = new KafkaDispatcher<>()) {

			while (true) {
				var records = consumer.poll(Duration.ofMillis(100));

				if (!records.isEmpty()) {
					System.out.println("Encontrei " + records.count() + " registro(s)");

					for (var record : records) {
						try {
							this.parse.consume(record);
						} catch (Exception e) {
							e.printStackTrace();
							var message = record.value();

							deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
									message.getId().continueWith("DeadLetter"),
									new GsonSerializer().serialize("", message));
						}
					}
				}
			}
		}
	}

	private Properties properties(Map<String, String> overrideProperties) {
		var properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		// Avisa ao broker que irá processar uma mensagem por vez.
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.putAll(overrideProperties);

		return properties;
	}

	@Override
	public void close() {
		this.consumer.close();
	}

}
