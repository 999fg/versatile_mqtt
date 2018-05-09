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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.JSONObject;
import java.util.Date;

public class VersatileMqttClient implements MqttCallback {
	MqttClient mClient;
	MqttConnectOptions connOpts;
	
	static final String BROCKER_URL = "tcp://127.0.0.1:1883";

	static final String DATABASE_URL = "jdbc:mysql://127.0.0.1:3306/versatile";
	static final String TABLE_NAME = "deviceInfo";

	static final Boolean subscriber = true;
	static final Boolean publisher = true;

	static final String s_Topic = "STATUS";
	static final String sACK_Topic = "STATUS_ACK";

	static Connection connection = null;
	static Statement stmt = null;
	
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
		
		if (topic.equals(sACK_Topic)) {
			JSONObject json = new JSONObject(new String(message.getPayload()));
			
			String deviceName = json.get("deviceName").toString();

			DeviceInfo devInfo = new DeviceInfo(deviceName, true, 0);
			try {
				connection = DriverManager.getConnection(DATABASE_URL, "root", "mysql");
				stmt = connection.createStatement();
				String sql = "select count(*) from "+TABLE_NAME+" where deviceName = \""+deviceName+"\"";
				ResultSet rs = stmt.executeQuery(sql);
				while(rs.next()){
					if (Integer.parseInt(rs.getString(1).toString()) > 0 ) {
						sql = devInfo.genUpdateQuery("deviceInfo");
					} else if (Integer.parseInt(rs.getString(1).toString()) == 0) {
						sql = devInfo.genInsertQuery("deviceInfo");
					}
				}
				stmt.executeUpdate(sql);

			} catch (SQLException se) {
				se.printStackTrace();
			}
			//while(rs.next()){
			//	System.out.println(rs.getString(1));
			//}
			//DeviceInfo devInfo = new DeviceInfo(new String(message.getPayload()), true, new java.text.SimpleDateFormat("HHmmss".format(new java.util.Date())));
		}
		
	}

    public static void main( String[] args ) {
		/*
		try {
			//connection = DriverManager.getConnection(DATABASE_URL, "root", "mysql");
			//stmt = connection.createStatement();

		} catch (SQLException se1) {
			se1.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (connection != null)
					connection.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		*/
    	VersatileMqttClient vmc = new VersatileMqttClient();
		vmc.runClient();
	}
	public void runClient() {
		String clientID = "VersatileMqttBrockerClient";

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
		
		MqttTopic topic = mClient.getTopic(s_Topic);

		if (subscriber) {
			try {
				int subQoS = 0;
				mClient.subscribe(sACK_Topic, subQoS);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (publisher) {
			while (true) {
				Boolean shouldHeartBeat = false;
				try {
					connection = DriverManager.getConnection(DATABASE_URL, "root", "mysql");
					stmt = connection.createStatement();
					String sql = "select deviceName, timeToLive from "+TABLE_NAME;
					ResultSet rs = stmt.executeQuery(sql);
					int timeNow = Integer.parseInt(new java.text.SimpleDateFormat("HHmmss").format(new java.util.Date()));
					if (!rs.isBeforeFirst()) {
						shouldHeartBeat = true;
					}
					while(rs.next()) {
						if (timeNow - Integer.parseInt(rs.getString(2).toString()) >= 10){
							shouldHeartBeat = true;
						} else {
							System.out.println(rs.getString(1).toString() + "'s timeToLive: " + rs.getString(2).toString());
							System.out.println("Current Time: "+timeNow);
							System.out.println("Difference: "+(timeNow - Integer.parseInt(rs.getString(2).toString())));
						}
					}

				} catch (SQLException se) {
					se.printStackTrace();
				}
				if (shouldHeartBeat) {
					String pubMessage = "{\"pubmessage\":" + 0 + "}";
					int pubQoS = 0;
					MqttMessage message = new MqttMessage(pubMessage.getBytes());
					message.setQos(pubQoS);
					message.setRetained(false);

					System.out.println("Publishing to topic \"" + topic + "\" qos " + pubQoS);
					MqttDeliveryToken token = null;
						
					try{
						token = topic.publish(message);
						token.waitForCompletion();
						//Thread.sleep(1000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		try {
			mClient.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		try {
			if (subscriber) {
				Thread.sleep(5000);
			}
			mClient.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
	}
}
