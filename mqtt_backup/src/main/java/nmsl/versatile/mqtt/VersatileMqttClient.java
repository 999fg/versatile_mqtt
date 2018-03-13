package nmsl.versatile.mqtt;
/*
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
*/
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class VersatileMqttClient implements MqttCallback {
	MqttClient mClient;
	MqttConnectOptions connOpts;
	
	static final String BROCKER_URL = "tcp://127.0.0.1:1883";

	static final Boolean subscriber = true;
	static final Boolean publisher = true;

	@Override
	public void connectionLost(Throwable t) {
		System.out.println("Connection Lost");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		System.out.println("Delivery Success");
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("-----------------------------------------------------");
		System.out.println("| Topic:" + topic); 
		System.out.println("| Message:" + new String(message.getPayload()));
		System.out.println("-----------------------------------------------------");
	}

    public static void main( String[] args ) {
    	VersatileMqttClient vmc = new VersatileMqttClient();
		vmc.runClient();
	}
	public void runClient() {
		String clientID = "VersatileMqttClientSample";
		connOpts = new MqttConnectOptions();

		connOpts.setCleanSession(true);
		connOpts.setKeepAliveInterval(30);
		
		try {
			mClient = new MqttClient(BROCKER_URL, clientID);
			mClient.setCallback(this);
			mClient.connect(connOpts);
		} catch (MqttException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		System.out.println("Connected to " + BROCKER_URL);
		
		String mTopic = "MQTT Examples";
		MqttTopic topic = mClient.getTopic(mTopic);

		if (subscriber) {
			try {
				int subQoS = 0;
				mClient.subscribe(mTopic, subQoS);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (publisher) {
			for (int i = 1; i <= 10; i++) {
				String pubMessage = "{\"pubmessage\":" + i + "}";
				int pubQoS = 0;
				MqttMessage message = new MqttMessage(pubMessage.getBytes());
				message.setQos(pubQoS);
				message.setRetained(false);

				System.out.println("Publishing to topic \"" + topic + "\" qos " + pubQoS);
				MqttDeliveryToken token = null;
				
				try{
					token = topic.publish(message);
					token.waitForCompletion();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		try {
			if (subscriber) {
				Thread.sleep(5000);
			}
			mClient.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
