# worker receives section of samples data from master, processes data, and each sends calculated partial histogram back to 
# master for final plot

import pika 
import pickle
import numpy as np 
import uproot
import awkward as ak
import vector 
import time 
import atlasopenmagic as atom 
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

# -----------------------------------------------------------------------------------------
# 1. setting up analysis functions and variables 
# -----------------------------------------------------------------------------------------
MeV = 0.001
GeV = 1.0

# Set luminosity to 36.6 fb-1, data size of the full release
lumi = 36.6

# Controls the fraction of all events analysed
fraction = 1.0 # reduce this is if you want quicker runtime (implemented in the loop over the tree)


variables = ['lep_pt','lep_eta','lep_phi','lep_e','lep_charge','lep_type','trigE','trigM','lep_isTrigMatched',
            'lep_isLooseID','lep_isMediumID','lep_isLooseIso','lep_type']
weight_variables =  ["filteff","kfac","xsec","mcWeight","ScaleFactor_PILEUP", "ScaleFactor_ELE", "ScaleFactor_MUON", "ScaleFactor_LepTRIGGER"]
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

# for MC weights calculation
def calc_weight(weight_variables, events):
    total_weight = lumi * 1000 / events["sum_of_weights"]
    for variable in weight_variables:
        total_weight = total_weight * abs(events[variable])
    return total_weight

# -----------------------------------------------------------------------------------------
# 3. defining function to process data once the data URL has been read and received
# -----------------------------------------------------------------------------------------

def process_file(file_URL, sample, bin_edges):

    tree = uproot.open(file_URL + ":analysis")

    total_hist = np.zeros(len(bin_edges) - 1)

    for data in tree.iterate(
        variables + weight_variables + ["sum_of_weights", "lep_n"],
        library='ak'
    ):

        # --- SAME CUTS AS NOTEBOOK ---
        data = data[cut_trig(data.trigE, data.trigM)]
        data = data[cut_trig_match(data.lep_isTrigMatched)]

        data['leading_lep_pt'] = data['lep_pt'][:,0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
        data['third_leading_lep_pt'] = data['lep_pt'][:,2]

        data = data[data['leading_lep_pt'] > 20]
        data = data[data['sub_leading_lep_pt'] > 15]
        data = data[data['third_leading_lep_pt'] > 10]

        data = data[ID_iso_cut(
            data.lep_isLooseID,
            data.lep_isMediumID,
            data.lep_isLooseIso,
            data.lep_isLooseIso,
            data.lep_type
        )]

        data = data[~cut_lep_type(data.lep_type)]
        data = data[~cut_lep_charge(data.lep_charge)]

        
        mass = calc_mass(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_e)

     
        if "data" not in sample.lower():
            weights = calc_weight(weight_variables, data)
            hist, _ = np.histogram(
                ak.to_numpy(mass),
                bins=bin_edges,
                weights=ak.to_numpy(weights)
            )
        else:
            hist, _ = np.histogram(
                ak.to_numpy(mass),
                bins=bin_edges
            )

        # adding all histograms of each sample in url
        total_hist += hist

    return total_hist 

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
# 5. establishing pika connection to send and receive messages from master
# -----------------------------------------------------------------------------------------

# rabbitmq broker on docker
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

# create connection to broker 
connection = rabbitmq_connect('rabbitmq')
channel = connection.channel()

# creating queues
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')
channel.basic_qos(prefetch_count=1)


# -----------------------------------------------------------------------------------------
# 6. define a function to call when message is received
# -----------------------------------------------------------------------------------------

def callback(ch, method, properties, body):
    # receives pickled url from master
    print(f' [x] Received task') 
   
   # unpickling task 
    print(f' [x] Making tasks readable') 
    task = pickle.loads(body)

    file_url = task['file']
    sample = task['sample']

    # processing data from url 
    print(f' [x] Processing {body}')
    histogram = process_file(file_url, sample, bin_edges)

    # creating histogram result for data 
    result = {'sample':sample, 'file': file_url, 'histogram' : histogram}

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

