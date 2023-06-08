package br.com.alura.ecommerce.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.MessageAdapter;

@SuppressWarnings("rawtypes")
public class GsonDeserializer implements Deserializer<Message> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();
	
	@Override
	public Message deserialize(String topic, byte[] data) {
		return gson.fromJson(new String(data), Message.class);
	}
}
