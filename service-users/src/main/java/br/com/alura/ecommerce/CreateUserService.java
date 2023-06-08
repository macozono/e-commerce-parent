package br.com.alura.ecommerce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.database.LocalDatabase;

public class CreateUserService implements ConsumerService<Order> {

	private final LocalDatabase database;

	private CreateUserService() throws SQLException {
		this.database = new LocalDatabase("users_database.db");
		this.database.createIfNotExists("create table Users (uuid varchar(200) primary key, email varchar(200))");
	}

	public static void main(String args[]) throws SQLException, ExecutionException, InterruptedException {
		new ServiceRunner<>(CreateUserService::new).start(1);
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record)
			throws IOException, InterruptedException, ExecutionException, SQLException {

		System.out.println("------------------------------------------");
		System.out.println("Processing new order, checking for new user");
		System.out.println(record.value());

		var message = record.value();
		var order = message.getPayload();

		if (isNewUser(order.getEmail())) {
			insertNewOrder(order.getEmail());
		}
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return CreateUserService.class.getSimpleName();
	}

	private void insertNewOrder(String email) throws SQLException {
		var statement = "insert into Users (uuid, email) values (?,?)";
		var uuid = UUID.randomUUID().toString();
		this.database.update(statement, uuid, email);
		
		System.out.println("Usuário uuid e " + email + " adicionado.");
	}

	private boolean isNewUser(String email) throws SQLException {
		var query = "select uuid from Users where email = ? limit 1";
		var rs = this.database.query(query, email);
		
		return !rs.next();
	}
}