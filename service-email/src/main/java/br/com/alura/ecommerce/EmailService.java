package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<Email> {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String args[]) throws ExecutionException, InterruptedException {
		new ServiceRunner(EmailService::new).start(5); 
	}
	
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}
	
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}
	
	public void parse(ConsumerRecord<String, Message<Email>> record) {
		System.out.println("------------------------------------------");
		System.out.println("Sending email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		
		// apenas para simular um "tempo" de leitura entre as mensagens...
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		
		System.out.println("Email sent");
	}
}
