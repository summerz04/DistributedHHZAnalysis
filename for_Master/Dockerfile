# master reads from ATLAS database and plots final histogram
FROM python:3.11

# set working directory to /app within the container  
WORKDIR /app

# copying pythons scripts into container
COPY master.py /app/master.py 

# downloading necessary libraries
RUN pip install --no-cache-dir numpy matplotlib atlasopenmagic pika awkward 

CMD ["python", "master.py"]
