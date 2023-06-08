package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderMain {

	public static void main(String args[]) throws InterruptedException, ExecutionException {
		var email = Math.random() + "@email.com";
		
		for (int i=0; i < 10; i++) {
			var id = new CorrelationId(NewOrderMain.class.getSimpleName());
			
			try (var orderDispatcher = new KafkaDispatcher<Order>()) {
				var orderId = UUID.randomUUID().toString();
				var amount = new BigDecimal(Math.random() * 5000 + 1);
				var order = new Order(orderId, amount, email);
				
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
			}
		}
	}
}
