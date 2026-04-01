# master script will just read all the data 
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app
# copying required python scripts into container 
COPY for_Worker/worker.py /app/worker.py 
COPY shared/analysis_functions.py /app/analysis_functions.py

#packages to install for worker scripts, processing the data
RUN pip install --no-cache-dir numpy pika uproot awkward vector 

CMD ["python", "worker.py"]
