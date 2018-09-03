# Dockerized RabbitMQ messaging system 

Simple producer/consumer running in Docker containers

## Prerequisites

- Install [Docker](https://docs.docker.com/installation/)
- Install [Compose](https://docs.docker.com/compose/install/)
- Install PyYaml `pip install pyyaml`


Start container

    $ docker-compose up --build  
    

## Start the management interface to see the message traffic
    
    http://localhost:15672 or http://127.0.0.1:15672/

