import context
import paho.mqtt.client as mqtt
import sys

def on_connect(mqttc, obj, flags, rc):
	print("rc: " + str(rc))


def on_message(mqttc, obj, msg):
	print ("----------------------------------------------")
	print ("| Topic:" + msg.topic)
	print ("| Message:" + msg.payload)
	print ("----------------------------------------------")
	
	if msg.topic == "STATUS":
		print ("STATUS Arrived!")
		pubMessage = '{"deviceName":"' + sys.argv[1]+ '"}'
		pubQoS = 0
		print ('Publishing to topic "' + 'STATUS_ACK' + '" qos ' + str(pubQoS))
		mqttc.publish("STATUS_ACK", pubMessage)

def on_publish(mqttc, obj, mid):
	print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
	print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
	print(string)

BROCKER_URL = "127.0.0.1"
BROCKER_PORT = 1883
sTopic = "STATUS"
sackTopic = "STATUS_ACK"
subQoS = 0

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

mqttc.connect(BROCKER_URL, BROCKER_PORT, 60)
mqttc.subscribe(sTopic, 0)
mqttc.loop_forever()

