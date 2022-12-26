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



################################################################################
class MqttBroker(object):
   #----------------------------------------------------------------------------
   def __init__(self, ipAddr, port, userName = None, password = None):
      self.ipAddr = ipAddr
      self.port = port
      self.userName = userName
      self.password = password

################################################################################
class MqttValueTrigger(object):
   #----------------------------------------------------------------------------
   def __init__(self, trigger_thresh, compare_trigger_thresh_str, time_thresh = 0, cancel_thresh = None, compare_cancel_thresh_str = None):
      self.trigger_thresh = trigger_thresh
      self.compare_trigger_thresh_str = compare_trigger_thresh_str

      self.cancel_thresh = cancel_thresh
      self.compare_cancel_thresh_str = compare_cancel_thresh_str

      # How long after the initial trigger condition before performing the command
      self.time_thresh = time_thresh

      # Current state parameters
      self.trigger_state = False
      self.trigger_start_time = None

   #----------------------------------------------------------------------------
   def updateValue(self, floatVal: float):
      triggered = False
      canceled = False

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

   #----------------------------------------------------------------------------
   def checkRunCommand(self, nowTime):
      retVal = False
      if self.trigger_state and (nowTime - self.trigger_start_time) > self.time_thresh:
         retVal = True
         if self.cancel_thresh == None:
            # Just acted on a trigger that has no cancel threshold (i.e. no way to un-trigger). Un-trigger now.
            self.trigger_state = False
      return retVal


################################################################################
class MqttTimeoutTrigger(object):
   #----------------------------------------------------------------------------
   def __init__(self, timeout_time):
      self.timeout_time = timeout_time

      # Start Triggered.
      self.trigger_state = True
      self.trigger_start_time = time.time()

   #----------------------------------------------------------------------------
   def updateValue(self, floatVal: float):
      self.trigger_start_time = time.time() # Reset the trigger time.

   #----------------------------------------------------------------------------
   def checkRunCommand(self, nowTime):
      retVal = False
      if self.trigger_state and (nowTime - self.trigger_start_time) > self.timeout_time:
         retVal = True
            
         self.trigger_state = False # Only one trigger for now.
      return retVal


################################################################################
class MqttAction(object):
   #----------------------------------------------------------------------------
   def __init__(self, topic, trigger):
      self.lock = threading.RLock() # recursive mutex lock

      self.topic = topic

      # Trigger object.
      self.trigger_object = trigger

      # Info about the command to run when the trigger has happened for long enough
      self.command = None

   #----------------------------------------------------------------------------
   # Run this when a new MQTT value has come in.
   def newValue(self, newValStr: str):
      # Convert to float
      floatVal = 0.0
      try:
         floatVal = float(newValStr)
      except:
         if newValStr.lower() == "true":
            floatVal = 1.0
         elif newValStr.lower() == "false":
            floatVal = 0.0
         else:
            print("Failed to convert '" + newValStr + "' to string.")
            return # TODO throw exception?
      
      # Check if the new value changes anything.
      with self.lock: # Lock the mutex
         self.trigger_object.updateValue(floatVal)

      # Check if any actions need to occur.
      self.checkForTriggerAction()

   #----------------------------------------------------------------------------
   def checkForTriggerAction(self):
      with self.lock: # Lock the mutex
         performAction = False
         nowTime = time.time()

         # Check if the conditions are right to run the command.
         performAction = self.trigger_object.checkRunCommand(nowTime)

         if performAction:
            print("Run Command!")
               


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
   actions.append( MqttAction("test/123", MqttTimeoutTrigger(10)) )


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

