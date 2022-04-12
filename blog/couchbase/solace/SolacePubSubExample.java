package blog.couchbase.solace;


import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class SolacePubSubExample {

	//sample message payload. JSON document as String
	//	{
	//	  "hotel_id": "hotel_10025",
	//	  "pets_ok": true,
	//	  "free_breakfast": true,
	//	  "free_internet": true,
	//	  "free_parking": true
	//	}

	private static String data = "{\"hotel_id\": \"hotel_10025\",\n"
			+ "\"pets_ok\": true,\n" + "\"free_breakfast\": true,\n"
			+ "\"free_internet\": true,\n" + "\"free_parking\": true\n}";

	public static void main(String[] args) {
		try {
			final JCSMPProperties properties = new JCSMPProperties();
			properties.setProperty(JCSMPProperties.HOST, "tcps://mrmhaca6xp596.messaging.solace.cloud:55443");
			properties.setProperty(JCSMPProperties.USERNAME, "solace-cloud-client");
			properties.setProperty(JCSMPProperties.PASSWORD, "yourpassword");
			properties.setProperty(JCSMPProperties.VPN_NAME, "couchbasedemo");
			JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();

			final Topic topic = JCSMPFactory.onlyInstance().createTopic("tutorial/topic");

			// ### Message Receiver - Subscribe to Topic ###
			final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
				@Override
				public void onReceive(BytesXMLMessage msg) {
					if (msg instanceof TextMessage) {
						System.out.printf("SUBSCRIBE: Message received: '%s'%n", ((TextMessage) msg).getText());
						UpdateHotelData.getInstance().upsertHotelData(((TextMessage) msg).getText());
					} else {
						System.out.println("Message received.");
					}
				}

				@Override
				public void onException(JCSMPException e) {
					System.out.printf("Consumer received exception: %s%n", e);
				}
			});
			session.addSubscription(topic);
			System.out.println("SUBSCRIBE: Connected. Awaiting message...");
			cons.start();





			// ### Message Producer - Send message to Topic
			XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
				@Override
				public void responseReceivedEx(Object key) {
					System.out.println("Producer received response for msg: " + key.toString());
				}

				@Override
				public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
					System.out.printf("Producer received error for msg: %s@%s - %s%n", key.toString(), timestamp,
							cause);
				}
			});

			// Send messages. Here we loop through and send the same message multiple times.
			for (int msgsSent = 0; msgsSent < 2; ++msgsSent) {
				TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
				msg.setText(data);
				System.out.printf("PUBLISH: Sending message '%s' to topic '%s'...%n", data, topic.getName());
				prod.send(msg, topic);
				//Gives us some time to follow the console logs
				Thread.sleep(3000);
			}

			cons.close();
			System.out.println("Exiting.");
			session.closeSession();

		} catch (InvalidPropertiesException e1) {
			e1.printStackTrace();
		} catch (JCSMPException e) {
			e.printStackTrace();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

}
