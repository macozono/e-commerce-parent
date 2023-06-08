package br.com.alura.ecommerce;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

@SuppressWarnings("rawtypes")
public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {

	@Override
	public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject obj = new JsonObject();
		
		obj.addProperty("type", message.getPayload().getClass().getName());
		obj.add("payload", context.serialize(message.getPayload()));
		obj.add("correlationId", context.serialize(message.getId()));
		
		return obj;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		
		var obj = json.getAsJsonObject();
		var payloadType = obj.get("type").getAsString();
		CorrelationId correlationId = context.deserialize(obj.get("correlationId"), CorrelationId.class);
		
		try {
			// maybe you want to use a "accept list"
			var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
			return new Message(correlationId, payload);
			
		} catch (ClassNotFoundException e) {
			throw new JsonParseException(e);
		}
	}
}
