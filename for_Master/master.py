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

# creating queues
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

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
data_urls = samples['Data']['list'][0:2] # distributing with 2 files to test it works 

print(f'[x] Got {len(data_urls)} URLs!')

#3. sending URLs to workers
for url in data_urls:
    task = {'URL': url}

    channel.basic_publish(exchange='',
                        routing_key='tasks',
                        body=pickle.dumps(url))

print('[x] Sent the URLs!')

# 4. listening to collect results from workers 

combined_results = []

def collect_results(ch, method, properties, body):
    result = pickle.loads(body)
    combined_results.append(result['histogram'])

    print(f'[x] Received {len(combined_results)} results')

    # checking if the task is done
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # stop consuming if all the urls have been collected 
    if len(combined_results) == len(data_urls):
        ch.stop_consuming()

channel.basic_consume(
    queue='results',
    on_message_callback=collect_results
)

channel.start_consuming()

final_histogram = np.sum(combined_results, axis=0)
print('[x] Final combined histogram is ready!')
