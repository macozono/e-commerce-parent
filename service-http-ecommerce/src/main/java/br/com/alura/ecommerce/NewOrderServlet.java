package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		var orderId = req.getParameter("uuid");
		var email = req.getParameter("email");
		var amount = new BigDecimal(req.getParameter("amount"));
		var order = new Order(orderId, amount, email);

		try (var database = new OrdersDatabase()) {
			
			if (database.saveNew(order)) {
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
				System.out.println("new order sent successfully");

				resp.setStatus(HttpServletResponse.SC_OK);
				resp.getWriter().write("new order sent successfully");				
			} else {
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.getWriter().write("old order received");				
			}

		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		} catch (SQLException e) {
			throw new ServletException(e);
		}
	}
}
