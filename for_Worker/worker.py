# receives the data15_periodD_url from master 

import pika 
import pickle
import numpy as np 
import uproot
import awkward as ak
import vector 
import atlasopenmagic as atom 
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
# -----------------------------------------------------------------------------------------
# 1. setting up analysis functions and variables 
# -----------------------------------------------------------------------------------------
MeV = 0.001
GeV = 1.0

variables = ['lep_pt','lep_eta','lep_phi','lep_e','lep_charge','lep_type','trigE','trigM','lep_isTrigMatched',
            'lep_isLooseID','lep_isMediumID','lep_isLooseIso','lep_type']

# -----------------------------------------------------------------------------------------
# 2. defining analysis functions
# -----------------------------------------------------------------------------------------

def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

# Cut lepton charge
def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_e):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_e})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M # .M calculates the invariant mass
    return invariant_mass


def cut_trig_match(lep_trigmatch):
    trigmatch = lep_trigmatch
    cut1 = ak.sum(trigmatch, axis=1) >= 1
    return cut1

def cut_trig(trigE,trigM):
    return trigE | trigM


def ID_iso_cut(IDel,IDmu,isoel,isomu,pid):
    thispid = pid
    return (ak.sum(((thispid == 13) & IDmu & isomu) | ((thispid == 11) & IDel & isoel), axis=1) == 4)

# -----------------------------------------------------------------------------------------
# 3. defining function to process data once the data URL has been read and received
# -----------------------------------------------------------------------------------------

def process_file(file_URL, bin_edges): # explicitly pass bin_edges for correct plotting 
    
    #debugging
    print(f'######DEBUG URL:{file_URL} ')
    
    tree = uproot.open(file_URL + ":analysis")
    print(f'There are {tree.num_entries} entries in this dataset.')

    sample_data = []

    for data in tree.iterate(variables, library='ak'):
        data = data[~cut_lep_type(data['lep_type'])]
        data = data[~cut_lep_charge(data['lep_charge'])]
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_e'])
        sample_data.append(data)

    all_events = ak.concatenate(sample_data)
    print(f"Events after cuts: {len(all_events)}", flush=True)

    data_x,_ = np.histogram(ak.to_numpy(all_events['mass']),
                            bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data
    # returning histogram data
    return data_x
# -----------------------------------------------------------------------------------------
# 4. defining variables for histogram, might create a separate .py file for this later on
# -----------------------------------------------------------------------------------------
xmin = 80 * GeV
xmax = 250 * GeV

# Histogram bin setup
step_size = 2.5 * GeV

bin_edges = np.arange(start=xmin, # The interval includes this value
                    stop=xmax+step_size, # The interval doesn't include this value
                    step=step_size ) # Spacing between values

## editing bin_centres for plotting,
bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2 
    
# -----------------------------------------------------------------------------------------
# 5. PIKA STUFF:
# establishing pika connection to send and receive messages from master
# -----------------------------------------------------------------------------------------
#rabbitmq connection on machine
#params = pika.ConnectionParameters('localhost')

# rabbitmq broker on docker
params = pika.ConnectionParameters('rabbitmq')


# create connection to broker 
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

# create the queue, if it doesn't already exist
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

# -----------------------------------------------------------------------------------------
# 6. define a function to call when message is received
# -----------------------------------------------------------------------------------------

def callback(ch, method, properties, body):
    # receives pickled url from master
    print(f' [x] Received task') 
    
    # unpickling url
    #tasks = pickle.loads(body)

    
    print(f' [x] Making tasks readable') 
    # decoded_url = tasks['file url']

    # unpickling url 
    decoded_url = pickle.loads(body)

    # processing data from url 
    print(f' [x] Processing {body}')
    histogram = process_file(decoded_url, bin_edges)

    # creating histogram result for data 
    result = {'histogram' : histogram}

    # setup to publish results back to the master    
    channel.basic_publish(exchange='',
                    routing_key='results',
                    body=pickle.dumps(result)
                    )
    print('[x] Sent result back')

    ch.basic_ack(delivery_tag=method.delivery_tag)

# setup to listen for messages on queue 'tasks'
channel.basic_consume(queue='tasks',
                        on_message_callback=callback)
    

# log message to show we've started listening 
print('Waiting for messages. To exit, press CTRL+C')

channel.start_consuming()

