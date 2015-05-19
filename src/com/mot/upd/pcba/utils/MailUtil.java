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
import javax.mail.internet.AddressException;
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


	public static  void sendEmail(String serialNoIn,String SerialNoOut)throws Exception{

		final String USER_NAME = bundle.getString("username");
		final String server=bundle.getString("server");
		final String portNO=bundle.getString("portNO");
		final String RECIPIENT=bundle.getString("recipient");
		String subject=bundle.getString("emailSubject");

		String body=bundle.getString("emailBody");
		body = MessageFormat.format(body, serialNoIn,SerialNoOut);


		String from = USER_NAME;

		String[] to = { RECIPIENT }; // list of recipient email addresses

		Properties props = System.getProperties();
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", server);
		props.put("mail.smtp.user", from);
		props.put("mail.smtp.port", portNO);
		props.put("mail.smtp.auth", "false");

		Session session = Session.getDefaultInstance(props);
		MimeMessage message = new MimeMessage(session);

		try {
			message.setFrom(new InternetAddress(from));
			InternetAddress[] toAddress = new InternetAddress[to.length];

			// To get the array of addresses
			for( int i = 0; i < to.length; i++ ) {
				toAddress[i] = new InternetAddress(to[i]);
			}

			for( int i = 0; i < toAddress.length; i++) {
				message.addRecipient(Message.RecipientType.TO, toAddress[i]);
			}

			message.setSubject(subject);  
			message.setContent(body, "text/html");
			Transport transport = session.getTransport("smtp");
			transport.connect(server, from);
			transport.sendMessage(message, message.getAllRecipients());
			transport.close();
			logger.info("Sent an E-Mail successfully....");
		}
		catch (AddressException ae) {
			logger.info("Reading AddressException"+ae.getMessage());
			ae.printStackTrace();
		}
		catch (MessagingException me) {
			logger.info("Reading MessagingException:"+me.getMessage());
			me.printStackTrace();
		}

	} 

}
