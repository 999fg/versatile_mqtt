import context
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import time
import json
import datetime

deviceInfoTTL = {}

def on_connect(mqttc, obj, flags, rc):
	print("rc: " + str(rc))
	mqttc.subscribe("STATUS_ACK", qos=0)

def on_message(mqttc, obj, msg):
	global deviceInfoTTL
	print ("----------------------------------------------")
	print ("| Topic:" + msg.topic)
	print ("| Message:" + msg.payload)
	print ("----------------------------------------------")
	
	if msg.topic == "STATUS_ACK":
		print ("STATUS_ACK Arrived!")
		d = json.loads(msg.payload)
		print (d["deviceName"])
		deviceInfoTTL[d["deviceName"]] = int(round(time.time() * 1000))
	print deviceInfoTTL

def on_publish(mqttc, obj, mid):
	print("mid: " + str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
	print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
	print(string)


mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

BROCKER_URL = "127.0.0.1"
BROCKER_PORT = 1883
sTopic = "STATUS"
sackTopic = "STATUS_ACK"
subQoS = 0
pubQoS = 0

mqttc.connect(BROCKER_URL, BROCKER_PORT, 60)
mqttc.loop_start()


while True:
	shouldHeartBeat = False
	timeNow = int(round(time.time() * 1000))
	if len(deviceInfoTTL) == 0:
		shouldHeartBeat = True
	else:
		for (key, value) in deviceInfoTTL.iteritems():
			if timeNow - value >= 10000:
				shouldHeartBeat = True
			else:
				print ("")
				print (key + "'s timetolive: " + str(datetime.datetime.fromtimestamp(float(value)/1000).strftime('%Y-%m-%d %H:%M:%S')))
				print ("current time: " + str(datetime.datetime.fromtimestamp(float(timeNow)/1000).strftime('%Y-%m-%d %H:%M:%S')))
				print ("Difference: " + str((timeNow - value)/1000)) + " seconds"
				print ("")
	if shouldHeartBeat:
		pubMessage = '{"pubMessage":'+str(0)+'}'
		pubQoS = 0
		print ('Publishing to topic "' + sTopic + '" qos ' + str(pubQoS))
		mqttc.publish(sTopic, pubMessage, qos=pubQoS)
	time.sleep(1)
