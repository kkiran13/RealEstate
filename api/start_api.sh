#!/bin/bash

docker ps -q -a | xargs docker rm

cd dockerfiles ; docker-compose up --build