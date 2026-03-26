# master script will just read all the data 
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app
# copying master.py script
COPY for_Master/master.py /app/master.py 
RUN pip install --no-cache-dir numpy matplotlib atlasopenmagic pika 

CMD ["python", "master.py"]
