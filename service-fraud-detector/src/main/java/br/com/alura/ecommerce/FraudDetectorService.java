package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.database.LocalDatabase;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements ConsumerService<Order> {
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	
	private final LocalDatabase database;
	
	private FraudDetectorService() throws SQLException {
		this.database = new LocalDatabase("frauds_database.db");
		this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key, is_fraud boolean)");
	}

	public static void main(String args[]) throws ExecutionException, InterruptedException {
		new ServiceRunner<>(FraudDetectorService::new).start(1);
	}

	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		System.out.println(record.topic());
		
		var message = record.value();
		var order = message.getPayload();
		
		if (wasProcessed(order)) {
			System.out.println("Order " + order.getOrderId() + " has already processed.");
			return;
		}
		
		// apenas para simular um "tempo" de leitura entre as mensagens...
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		
		if (isFraud(order)) {
			database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
			// fingindo que a fraude seja >= 4500.
			System.out.println("Order is a fraud");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);			
		} else {
			database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
			System.out.println("Order processed");
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);			
		}
	}

	private boolean wasProcessed(Order order) throws SQLException {
		var rs = database.query("select uuid from Orders where uuid =? limit 1", order.getOrderId());
		return rs.next();
	}

	private boolean isFraud(Order order) throws InterruptedException, ExecutionException {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}
}