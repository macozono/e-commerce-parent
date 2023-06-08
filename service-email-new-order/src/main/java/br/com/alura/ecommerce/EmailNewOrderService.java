package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {
	
	private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String args[]) throws ExecutionException, InterruptedException {
		new ServiceRunner(EmailNewOrderService::new).start(1);
	}
	
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}
	
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, preparing email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		System.out.println(record.topic());
		
		var message = record.value();
		var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
		var order = message.getPayload();
		var subject = "Test";
		var body = "Thank you for your oder! We are processing your order!";
		var emailCode = new Email(subject, body);
		
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
	}
}