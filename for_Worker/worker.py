# receives the data15_periodD_url from master 

import pika 
import pickle
import numpy as np 
import uproot
import awkward as ak
import vector 

# 1. establishing pika connection to receive messages from master

#rabbitmq connection on machine
params = pika.ConnectionParameters('localhost')

# create connection to broker 
connection = pika.BlockingConnection(params)
channel = connection.channel()

# create the queue, if it doesn't already exist
channel.queue_declare(queue='data URL')

# define a function to call when message is received
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

# setup to listen for messages on queue 'messages'
channel.basic_consume(queue='data URL',
                      auto_ack=True,
                      on_message_callback=callback)

# log message to show we've started listening 
print('Waiting for messages. To exit, press CTRL+C')

channel.start_consuming()