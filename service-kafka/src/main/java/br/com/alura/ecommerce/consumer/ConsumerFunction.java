package br.com.alura.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.Message;

public interface ConsumerFunction<T> {

	void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
