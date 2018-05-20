#!/bin/bash

docker pull haproxy

docker run -d --network rabbitmq-net --name rabbitmq-haproxy -p 5672:5672 -v ${PWD}:/usr/local/etc/haproxy:ro haproxy
