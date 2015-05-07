/**
 * 
 */
package com.mot.upd.pcba.utils;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.PropertyResourceBundle;

import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Logger;
/**
 * @author rviswa
 *
 */
public class MailUtil {

	private static Logger logger = Logger.getLogger(MailUtil.class);
	private static PropertyResourceBundle bundle = InitProperty.getProperty("pcbaMail.properties");


	public static  boolean sendEmail(String serialNoIn,String SerialNoOut)throws Exception{

		boolean isMailSent = false;
		String email = bundle.getString("email");
		final String username = bundle.getString("username");
		final String password = bundle.getString("password");
		String server=bundle.getString("server");
		String portNO=bundle.getString("portNO");
		// Sender's email ID needs to be mentioned
		String from = bundle.getString("from");
		String emailSubject=bundle.getString("emailSubject");
		String emailBody=bundle.getString("emailBody");
		emailBody = MessageFormat.format(emailBody, serialNoIn,SerialNoOut);

		// Recipient's email ID needs to be mentioned.
		String to = email;
		// Get system properties
		Properties properties = System.getProperties();
		// Setup mail server
		properties.setProperty("mail.smtp.auth", "true");
		properties.setProperty("mail.smtp.starttls.enable", "true");
		properties.setProperty("mail.smtp.host", server);
		properties.setProperty("mail.smtp.port", portNO);

		// Get the default Session object.
		Session session = Session.getInstance(properties,
				new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try{
			// Create a default MimeMessage object.
			Multipart mp = new MimeMultipart();
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(from));
			message.setSubject(emailSubject);
			// Set To: header field of the header.
			message.addRecipient(Message.RecipientType.TO,
					new InternetAddress(to));
			BodyPart messageBodyPart = new MimeBodyPart();

			messageBodyPart.setContent(emailBody, "text/html");
			mp.addBodyPart(messageBodyPart);
			// Set Subject: header field
			message.setContent(mp);



			// Send message
			Transport.send(message);
			System.out.println("Sent message successfully....");
			isMailSent = true;
		}catch (MessagingException mex) {
			mex.printStackTrace();
			throw mex;
		}


		return isMailSent;

	} 

}
