package br.com.alura.ecommerce.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

	private final Connection connection;
	
	public LocalDatabase(String name) throws SQLException {
		String url = "jdbc:sqlite:target/" + name;
		connection = DriverManager.getConnection(url);
	}
	
	// yes, this is way too generic
	// according to your dtabase to evict sql injection
	public void createIfNotExists(String sql) {
		try {
			connection.createStatement().execute(sql);
		} catch (SQLException ex) {
			// cuidado que a query pode estar realmente sendo escrita errada.
			ex.printStackTrace();
		}		
	}

	public void update(String statement, String... params) throws SQLException {
		var preparedStatement = prepare(statement, params);
		
		preparedStatement.execute();		
	}

	private PreparedStatement prepare(String statement, String... params) throws SQLException {
		var preparedStatement = connection.prepareStatement(statement);

		for (int i=0; i < params.length; i++) {
			preparedStatement.setString(i+1, params[i]);	
		}
		return preparedStatement;
	}

	public ResultSet query(String query, String... params) throws SQLException {
		var preparedStatement = prepare(query, params);
		
		var results = preparedStatement.executeQuery();
		return results;
	}

	public void close() throws SQLException {
		this.connection.close();
	}
}
