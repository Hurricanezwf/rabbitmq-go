#!/bin/bash

docker pull rabbitmq:3.7.4-management

docker run -d --hostname zwf --name rabbitmq -p 8088:15672 -p 5672:5672 -e RABBITMQ_DEFAULT_USER=zwf -e RABBITMQ_DEFAULT_PASS=123456 rabbitmq:3.7.4-management

docker ps -a |grep rabbitmq
