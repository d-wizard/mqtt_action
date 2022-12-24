# 
# MIT License
# 
# Copyright (c) 2022 Dan Williams
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import time
import json
import random
import threading

from paho.mqtt import client as mqtt_client


class MqttBroker(object):
   def __init__(self, ipAddr, port, userName = None, password = None):
      self.ipAddr = ipAddr
      self.port = port
      self.userName = userName
      self.password = password

class MqttAction(object):
   def __init__(self):
      self.lock = threading.RLock() # recursive mutex lock

      self.topic = None

      # What trigger should cause and action
      self.trigger_value_thresh = False
      self.trigger_missing_msg_time = False

      # How long after the initial trigger condition before performing the command
      self.time_thresh = 0

      # Parameters used when trigger_value_thresh is true
      self.trigger_thresh = 0
      self.compare_trigger_thresh_str = ">" # >, <, >=, <=
      self.cancel_thresh = None # Hysteresis
      self.compare_cancel_thresh_str = ">" # >, <, >=, <=

      # Info about the command to run when the trigger has happened for long enough
      self.command = None

      self.trigger_state = False
      self.trigger_start_time = None

   # Since the constructor doesn't have enough info to fill all the member variables 
   # in, this can be called to start things off after all the member variables have 
   # been manually filled in.
   def start(self):
      if self.trigger_missing_msg_time:
         self.trigger_state = True
         self.trigger_start_time = time.time()

   
   def newValue(self, newValStr: str):
      floatVal = 0.0
      try:
         floatVal = float(newValStr)
      except:
         if newValStr.lower() == "true":
            floatVal = 1.0
         elif newValStr.lower() == "false":
            floatVal = 0.0
         else:
            return # TODO throw exception?
      
      self.checkForTriggers(floatVal)
      self.checkForTriggerAction()

   def checkForTriggers(self, floatVal: float):
      with self.lock: # Lock the mutex
         triggered = False
         canceled = False
         if self.trigger_value_thresh:
            if (self.compare_trigger_thresh_str == ">"  and floatVal > self.trigger_thresh) or \
               (self.compare_trigger_thresh_str == "<"  and floatVal < self.trigger_thresh) or \
               (self.compare_trigger_thresh_str == ">=" and floatVal >= self.trigger_thresh) or \
               (self.compare_trigger_thresh_str == "<=" and floatVal <= self.trigger_thresh):
               triggered = True
            elif (self.cancel_thresh != None) and (
               (self.compare_cancel_thresh_str == ">"  and floatVal > self.cancel_thresh) or \
               (self.compare_cancel_thresh_str == "<"  and floatVal < self.cancel_thresh) or \
               (self.compare_cancel_thresh_str == ">=" and floatVal >= self.cancel_thresh) or \
               (self.compare_cancel_thresh_str == "<=" and floatVal <= self.cancel_thresh) ):
               canceled = True

         # Update member variables.
         if triggered:
            self.trigger_state = True
            self.trigger_start_time = time.time()
         elif canceled:
            self.trigger_state = False

   def checkForTriggerAction(self):
      with self.lock: # Lock the mutex
         nowTime = time.time()

         if self.trigger_state and (nowTime - self.trigger_start_time) > self.time_thresh:
            # Perform command
            print("Performing Command")
         
            if self.trigger_value_thresh:
               if self.cancel_thresh == None:
                  # Just acted on a trigger that has no cancel threshold (i.e. no way to un-trigger). Un-trigger now.
                  self.trigger_state = False
            elif self.trigger_missing_msg_time:
               self.trigger_state = False
               



################################################################################
# MQTT Functions
################################################################################
def mqttConnect(mqttBroker: MqttBroker):
   def mqtt_onConnect(client, userdata, flags, rc):
      if rc == 0:
         print("Connected to MQTT Broker!")
      else:
         print("Failed to connect, return code " + str(rc))

   random.seed() # Make sure to start random number generator at a new point each time this is run.
   mqttClientId = f'mqttAction-{random.randint(0, 1000000)}'
   
   # Create the MQTT client.
   client = mqtt_client.Client(mqttClientId)
   if mqttBroker.userName != None and mqttBroker.password != None:
      client.username_pw_set(mqttBroker.userName, mqttBroker.password)

   client.on_connect = mqtt_onConnect
   client.connect(mqttBroker.ipAddr, mqttBroker.port)
   return client

def mqttSubscribe(client: mqtt_client, action: MqttAction):
   def mqtt_onMessage(client, userdata, msg):
      action.newValue(msg.payload.decode())
      #print(msg.payload.decode())
      #logMqttData(msg.payload.decode(), ctrlSettings)

   print("Subscribing to topic: " + action.topic)
   client.subscribe(action.topic)
   client.on_message = mqtt_onMessage


################################################################################
# Program Start
################################################################################
if __name__ == "__main__":
   broker = MqttBroker("127.0.0.1", 1883)

   actions = []
   action = MqttAction()
   action.topic = "test/123"
   #action.trigger_value_thresh = True
   action.trigger_missing_msg_time = True
   action.trigger_thresh = 10
   action.time_thresh = 10
   action.start()
   actions.append(action)


   # Connect to MQTT Broker
   success = False
   while not success:
      try:
         mqttClient = mqttConnect(broker)
         success = True
      except:
         print("Failed to run mqttConnect()")
         time.sleep(15)

   # Start the Client loop
   success = False
   while not success:
      try:
         mqttClient.loop_start()
         success = True
      except:
         print("Failed to run mqttClient.loop_start()")
         time.sleep(15)

   # Subscribe to the topic(s)
   success = False
   for action in actions:
      while not success:
         try:
            mqttSubscribe(mqttClient, action)
            success = True
         except:
            print("Failed to run mqttSubscribe()")
            time.sleep(15)

   # Start Forever Loop. Messages from subscriptions are handled on other threads.
   # Handle timeouts here (i.e. actions that happen X amount of seconds after a trigger).
   while 1:
      time.sleep(1)
      for action in actions:
         action.checkForTriggerAction()

