# Distributed HHZ Analysis

> Special Core in Software Engineering and High Performance Computing for Scientists 2025, University of Bristol

This repository contains a distributed solution of ATLAS' Higgs boson (HZZ) analysis developed using the Docker Desktop tool. The project's aim is to use cloud technologies to create a theoretically scalable HZZ analysis processing system that requires as least human intervention as possible. 

## Usage instructions 
> To build and run containers: 
docker-compose can be used to build all containers using the command below:

```bash
docker-compose build 
```
The containers can be run by using
```bash 
docker-compose up
```
and the containers can be stopped using 
```bash
docker-compose down
```
> To create a distributed multi-node system using docker swarm
The build-in Docker Swarn Engine within Docker is a tool for container orchestration, and useful for scaling multi-node systems. To use Docker Swarm, see the commands and explanations below: 

1. Initialise a swarm:
```bash 
docker swarm init 
```

2. Create a RabbitMQ service:
```bash 
docker service create --name rabbitmq --network rabbit -p 5672:5672 rabbitmq:3-management
```

3. Build the master and worker images:
```bash 
docker build -t master ./for_Master/ # builds master image
docker build -t worker ./for_Worker/ # builds worker image
```
4. Create services for master and worker:
```bash
docker service create --name master --network rabbit master
docker service create --name worker1 --network rabbit worker
```
5. Number of workers can be scaled using: 
```bash
docker service scale worker=3
```
6. Logs for each service can be viewed using:
```bash
docker service logs -f worker1 # to see logs of worker 1
```



