# master file should read data, send data to workers, and then collect results back to plot the final histogram

# for now, i will simply just try to recreate the notebook, and distribute it to one worker

import pika
import pickle
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import atlasopenmagic as atom
import os

# 1. connecting to RabbitMQ
print('Master script is starting')

# testing on localhost first
params = pika.ConnectionParameters('localhost')

# creating connect to broker
connection = pika.BlockingConnection(params)
channel = connection.channel()

# creating queue
channel.queue_declare(queue='data URL')

# 2. get the data_15_periodD file URL
atom.set_release('2025e-13tev-beta')

# parameters for reading data
lumi = 6.6
fraction = 1.0
skim = 'exactly4lep'

# defining dictionary 
defs = {
    r'Data':{'dids':['data']},
    r'Background $Z,t\bar{t},t\bar{t}+V,VVV$':{'dids': [410470,410155,410218,
                                                        410219,412043,364243,
                                                        364242,364246,364248,
                                                        700320,700321,700322,
                                                        700323,700324,700325], 'color': "#6b59d3" }, # purple
    r'Background $ZZ^{*}$':     {'dids': [700600],'color': "#ff0000" },# red
    r'Signal ($m_H$ = 125 GeV)':  {'dids': [345060, 346228, 346310, 346311, 346312,
                                          346340, 346341, 346342],'color': "#00cdff" },# light blue
}

samples   = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)
data15_periodD_url = samples['Data']['list'][0]

print('[x] Got the URL!')

#3. sending URL to worker 
channel.basic_publish(exchange='',
                      routing_key='data URL',
                      body=pickle.dumps(data15_periodD_url))

print('[x] Sent the URL!')
