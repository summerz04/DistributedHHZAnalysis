# master script will just read all the data 
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app

# copying pythons scripts into container
COPY master.py /app/master.py 
#COPY shared/analysis_functions.py /app/analysis_functions.py

RUN pip install --no-cache-dir numpy matplotlib atlasopenmagic pika 

CMD ["python", "master.py"]
