package br.com.alura.ecommerce;

public class Email {

	private final String subject;
	private final String body;
	
	Email(String subject, String body) {
		this.subject = subject;
		this.body = body;
	}

	public String getSubject() {
		return subject;
	}

	public String getBody() {
		return body;
	}
	
	@Override
	public String toString() {
		return "Email [subject=" + subject + ", body=" + body + "]";
	}
}
