import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import json
import requests
import math
import pandas as pd
# This is the Subscriber
#hostname
broker="127.0.0.1"
#port
port=1883
#time to live
timelive=60
client = MongoClient(
    "mongodb+srv://script-user:ukNjS9pzuCFVamJ3@mokosmartdata.kjrh6.mongodb.net/beacons?retryWrites=true&w=majority"
)
my_db = client["beacons"]

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("/newData")
def on_message(client, userdata, msg):
    if msg is None or msg.payload is None:
      return
    now = datetime.utcnow()
    data = msg.payload.decode() 
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

      #remove top and bottom 10% of pair's rssi valies (remove outliers)
      pair_rssi_values.sort()
      num_to_remove = math.ceil(len(pair_rssi_values)*.10)
      #print("hi",(pair_rssi_values))
      reduced_pair_rssi_values = pair_rssi_values[num_to_remove:len(pair_rssi_values)-num_to_remove]
      #print("reduced",(reduced_pair_rssi_values))

      if len(reduced_pair_rssi_values) == 0:
        continue

      pair_rssi_avg = (sum(reduced_pair_rssi_values) / len(reduced_pair_rssi_values))
      print("avg",pair_rssi_avg)
      #convert rssi to meters using cubic regression
      #meters = pow(10,((-65-(pair_rssi_avg))/(10*2))) #https://medium.com/technology-hits/convert-rssi-value-of-the-ble-bluetooth-low-energy-beacons-to-meters-63259f307283
      a = 0.00015548865613013163
      b = 0.03110638083097683193
      c = 1.83187605430430267006
      d = 34.09275255328975617886
      meters = a*pow(pair_rssi_avg,3) + b*pow(pair_rssi_avg,2) + c*pair_rssi_avg + d 
      print(meters)
      
      upload_data.append({
        "mac_address": pair_mac,
        "meters": meters,
        "gateway": pair_gateway,
        "created_at": now,
      })

  
    print('\nupload to mongo',upload_data)
    my_db["raw_beacons_data"].insert_many(upload_data)


    url = 'https://position-api.herokuapp.com/clean_beacon_data'
    body = {'timestamp': str(now)}

    x = requests.post(url, json = body)

    print(x.text)




client = mqtt.Client()
client.connect(broker,port,timelive)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
