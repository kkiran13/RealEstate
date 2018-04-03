#!/bin/bash

docker stop $(docker ps -a -q)
docker ps -q -a | xargs docker rm