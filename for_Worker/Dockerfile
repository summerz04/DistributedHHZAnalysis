# master script will just read all the data 
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app
# copying master.py script
COPY for_Worker/worker.py /app/worker.py 

#packages to install for worker scripts, processing the data
RUN pip install --no-cache-dir numpy matplotlib pika uproot awkward vector 

CMD ["python", "worker.py"]
