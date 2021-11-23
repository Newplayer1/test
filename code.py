import csv
from paho.mqtt import client as mqtt
import os
import time


# Define event callbacks
def on_connect(client, userdata, flags, rc):
    print("rc: " + str(rc))


def on_message(client, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(client, obj, mid):
    print("mid: " + str(mid))


def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, obj, level, string):
    print(string)


mqttc = mqtt.Client()
# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Uncomment to enable debug messages
#mqttc.on_log = on_log

# Parse CLOUDMQTT_URL (or fallback to localhost)
#url_str = os.environ.get('driver.cloudmqtt.com', 'mqtt://localhost:1883')
#url = urlparse.urlparse(url_str)
topic = 'PourcentageBatterie'

# Connect
mqttc.username_pw_set("dhvvrehu", "10T74SxY2TX2")
mqttc.connect("driver.cloudmqtt.com", 18909)

# Start subscribe, with QoS level 0
mqttc.subscribe(topic, 0)

# Publish a message

with open('data.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        col = ''.join(row).split(',')
        mqttc.publish('p_load', col[1])
        mqttc.publish('p_solar_panel', col[2])
        mqttc.publish('battery_SoC', col[3])
        mqttc.publish('p_mppt', col[6])
        mqttc.publish('p_battery', col[7])
        mqttc.publish('p_inv', col[8])
        mqttc.publish('p_charger', col[9])
        time.sleep(5)





