# master file should read data, send data to workers, and then collect results back to plot the final histogram

# for now, i will simply just try to recreate the notebook, and distribute it to one worker

import pika
import pickle
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import atlasopenmagic as atom
import awkward as ak
import time # for retrying RabbitMQ connection
import os
# -----------------------------------------------------------------------------------------
# 1. connecting to RabbitMQ
# -----------------------------------------------------------------------------------------

print('Master script is starting')

# testing on localhost first
# params = pika.ConnectionParameters('localhost')

# for using rabbitmq broker on docker 
params = pika.ConnectionParameters('rabbitmq')
def rabbitmq_connect(host, retries=5, delay=5):
    for i in range(retries):
        try:

    # creating connect to broker
            connection = pika.BlockingConnection(params)
            print('Connected to RabbitMQ successfully')
            return connection
        except Exception:
            print(f'Unable to connect to RabbitMQ')
            print(f'Retrying')
            time.sleep(delay)
    raise Exception('Could not connect to RabbitMQ')


connection = rabbitmq_connect('rabbitmq')
channel = connection.channel()

# creating queues
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

# -----------------------------------------------------------------------------------------
# 2. building dataset
# -----------------------------------------------------------------------------------------
atom.set_release('2025e-13tev-beta')

# parameters for reading data
lumi = 36.6 # Set luminosity to 36.6 fb-1, data size of the full release
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

samples  = atom.build_dataset(defs, skim=skim, protocol='https', cache=True)

# changing to deal with whole data dictionary 
print(f'[x] Got the whole dictionary!')

# -----------------------------------------------------------------------------------------
#3. sending tasks to workers
# -----------------------------------------------------------------------------------------

# set up to count all tasks to check whether all tasks have been assigned and collected by the end
total_tasks = 0
for sample_name, sample_info in samples.items():
    for file_url in sample_info['list']:
        task = {'sample': sample_name, 'file': file_url}
        channel.basic_publish(exchange='',
                            routing_key='tasks',
                            body=pickle.dumps(task))
        total_tasks+=1

print(f'[x] Sent {total_tasks} tasks to all workers!')

# -----------------------------------------------------------------------------------------
# 4. listening to collect results from workers 
# -----------------------------------------------------------------------------------------

# storing as dictionary to combine results with appropriate keys
combined_results = {s: [] for s in samples}
received_tasks = 0
def collect_results(ch, method, properties, body):
    global received_tasks
    result = pickle.loads(body)

    # sample processed 
    sample = result['sample']

    # histogram computed
    hist = result['histogram']

    combined_results[sample].append(hist)

    # receiving results for specific sample 
    received_tasks += 1
    print(f'[x] Received results for {sample}')

    # checking if the task is done
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # stop consuming if results from all tasks have been collected 
    if received_tasks == total_tasks:
        ch.stop_consuming()

channel.basic_consume(
    queue='results',
    on_message_callback=collect_results
)
channel.start_consuming()

# -----------------------------------------------------------------------------------------
# 5. combining results and MC weights calculated from workers 
# -----------------------------------------------------------------------------------------

final_histograms = {
    s: np.sum(hists, axis=0)
    for s, hists in combined_results.items()
}

print('[x] Final combined histogram is ready!')

# -----------------------------------------------------------------------------------------
# 6. defining histogram settings
# -----------------------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------------------
# 7. preparing for final histogram plot
# -----------------------------------------------------------------------------------------
# getting histogram data 
data_x = final_histograms['Data']
# Calculating statistical error
data_x_errors = np.sqrt(data_x) 

#signal_x = ak.to_numpy(final_histograms[r'Signal ($m_H$ = 125 GeV)']['mass']) # histogram the signal
#signal_weights = ak.to_numpy(final_histograms[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
#signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

mc_x = [] # define list to hold the Monte Carlo histogram entries
mc_weights = [] # define list to hold the Monte Carlo weights
mc_colors = [] # define list to hold the colors of the Monte Carlo bars
mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

for s in samples: # not awkward arrays anymore, loop over samples
    if s not in ['Data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
        mc_x.append((final_histograms[s]) ) # append to the list of Monte Carlo histogram entries
        mc_weights.append(np.ones_like(final_histograms[s])) # append to the list of Monte Carlo weights
        mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
        mc_labels.append( s ) # append to the list of Monte Carlo legend labels

# *************
# Main plot
# *************
fig, main_axes = plt.subplots(figsize=(12, 8))

# plot the data points
main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                    fmt='ko', # 'k' means black and 'o' is for circles
                    label='Data')

# plotting must be modified, as collected results from workers are histograms already

#mc_heights = main_axes.bar(x=mc_x, bins=bin_edges,
#                            weights=mc_weights, stacked=True,
#                            color=mc_colors, label=mc_labels )

# creating empty histogram canvas to plot stacked histograms correctly
mc_x_tot =  np.zeros_like(bin_centres)

for i, s in enumerate(samples):
    if s not in ['Data', r'Signal ($m_H$ = 125 GeV)']:
        hist = final_histograms[s]

        main_axes.bar(bin_centres, hist, bottom=mc_x_tot,
        width=step_size, color = samples[s]['color'], label=s)

        mc_x_tot += hist

mc_x_err = np.sqrt(mc_x_tot)

# plot the signal bar
signal_hist = final_histograms[r'Signal ($m_H$ = 125 GeV)']
main_axes.bar(x=bin_centres, height=signal_hist, bottom=mc_x_tot, width=step_size,
              color=samples[r'Signal ($m_H$ = 125 GeV)']['color'],
              label=r'Signal ($m_H$ = 125 GeV)')
#signal_heights = main_axes.hist(signal_hist, bins=bin_edges, bottom=mc_x_tot,
#                weights=signal_weights, color=signal_color,
#                label=r'Signal ($m_H$ = 125 GeV)')

# plot the statistical uncertainty
main_axes.bar(x =bin_centres, # x
                height = 2*mc_x_err, # heights
                alpha=0.5, # half transparency
                bottom=mc_x_tot-mc_x_err, color='none',
                hatch="////", width=step_size, label='Stat. Unc.' )

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
main_axes.set_ylim( bottom=0, top=np.amax(data_x)*2.0 )

# add minor ticks on y-axis for main axes
main_axes.yaxis.set_minor_locator( AutoMinorLocator() )

# Add text 'ATLAS Open Data' on plot
plt.text(0.1, # x
            0.93, # y
            'ATLAS Open Data', # text
            transform=main_axes.transAxes, # coordinate system used is that of main_axes
            fontsize=16 )

# Add text 'for education' on plot
plt.text(0.1, # x
            0.88, # y
            'for education', # text
            transform=main_axes.transAxes, # coordinate system used is that of main_axes
            style='italic',
            fontsize=12 )

# Add energy and luminosity
lumi_used = str(lumi*fraction) # luminosity to write on the plot
plt.text(0.1, # x
            0.82, # y
            r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
            transform=main_axes.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

# Add a label for the analysis carried out
plt.text(0.1, # x
            0.76, # y
            r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text
            transform=main_axes.transAxes,fontsize=16 ) # coordinate system used is that of main_axes

# draw the legend
my_legend = main_axes.legend( frameon=False, fontsize=16 ) # no box around the legend

#---------
#saving histogram to container and local machine
#---------
import os
# create directory IF it doesnt exist already 
os.makedirs('/app/data', exist_ok=True)
plt.savefig('/app/data/2worker_final_mc_histogram.png')
print('\n Done, histogram is saved as 2worker_final_mc_histogram.png')
