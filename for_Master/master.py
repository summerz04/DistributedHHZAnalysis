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
    # task = {'URL': url}
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

## histogram settings
MeV = 0.001
GeV = 1.0
xmin = 80 * GeV
xmax = 250 * GeV

# Histogram bin setup
step_size = 2.5 * GeV
bin_edges = np.arange(start=xmin, # The interval includes this value
                    stop=xmax+step_size, # The interval doesn't include this value
                    step=step_size ) # Spacing between values
bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                        stop=xmax+step_size/2, # The interval doesn't include this value
                        step=step_size ) # Spacing between values
# *************
# Main plot
# *************
main_axes = plt.gca() # get current axes

# plot the data points
main_axes.errorbar(x=bin_centres, y=final_histogram,
                    fmt='ko', # 'k' means black and 'o' is for circles
                    label='Data')

# set the x-limit of the main axes
main_axes.set_xlim( left=xmin, right=xmax )

# separation of x axis minor ticks
main_axes.xaxis.set_minor_locator( AutoMinorLocator() )

# set the axis tick parameters for the main axes
main_axes.tick_params(which='both', # ticks on both x and y axes
                        direction='in', # Put ticks inside and outside the axes
                        top=True, # draw ticks on the top axis
                        right=True ) # draw ticks on right axis

# x-axis label
main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                    fontsize=13, x=1, horizontalalignment='right' )

# write y-axis label for main axes
main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                        y=1, horizontalalignment='right')

# set y-axis limits for main axes
main_axes.set_ylim( bottom=0, top=np.amax(final_histogram)*1.6 )

# add minor ticks on y-axis for main axes
main_axes.yaxis.set_minor_locator( AutoMinorLocator() )

# draw the legend
main_axes.legend( frameon=False ); # no box around the legend

plt.savefig('combined_test_histogram.png')
print('\n Done, histogram is saved as combined_test_histogram.png')
