# master script will just read all the data 
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app
# copying required python scripts into container 
# have to build from within for_Worker 
COPY worker.py /app/worker.py 

#packages to install for worker scripts, processing the data
RUN pip install --no-cache-dir numpy pika uproot awkward vector matplotlib atlasopenmagic aiohttp requests 

CMD ["python", "worker.py"]
