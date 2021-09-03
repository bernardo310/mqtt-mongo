import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import json
# This is the Subscriber
#hostname
broker="127.0.0.1"
#port
port=1883
#time to live
timelive=60
client = MongoClient(
    "mongodb+srv://script-user:chilaquilesconpollo@mokosmartdata.kjrh6.mongodb.net/"
)
my_db = client["beacons"]

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("/newData")
def on_message(client, userdata, msg):
    now = datetime.now()
    data = msg.payload.decode() #TODO validate that payload is array, error when scan is turned off in mokoscanner app
    data = json.loads(data)
    print('recieved new data',data)
    #find unique beacon-gateway macadresses pair
    unique_beacon_gateway = set()
    for message in data:
      mac_address = message[0]
      gateway = message[2]
      unique_beacon_gateway.add(mac_address+"-"+gateway)

    #iterate the pairs data and reduce to 1 object with its rssi average
    upload_data = []
    for pair in unique_beacon_gateway:
      #calculate average this beacon-gateway pair
      pair_rssi_values = []
      for message in data:
        #iterate all messages to find this pair's messages and add rssi to list
        mac_address = message[0]
        rssi = message[1]
        gateway = message[2]     
        if mac_address+"-"+gateway == pair:
          pair_rssi_values.append(int(rssi))

      #add to list this pair's data to be uploaded to mongo    
      pair_mac = pair.split('-')[0]
      pair_gateway = pair.split('-')[1]
      pair_rssi_avg = int(sum(pair_rssi_values) / len(pair_rssi_values))
      #convert rssi to meters
      meters = pow(10,((-65-(pair_rssi_avg))/(10*2))) #https://medium.com/technology-hits/convert-rssi-value-of-the-ble-bluetooth-low-energy-beacons-to-meters-63259f307283

      upload_data.append({
        "mac_address": pair_mac,
        "rssi": meters,
        "gateway": pair_gateway,
        "created_at": now,
      })


    print('upload to mongo',upload_data)
    my_db["raw_beacons_data"].insert_many(upload_data)





client = mqtt.Client()
client.connect(broker,port,timelive)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
