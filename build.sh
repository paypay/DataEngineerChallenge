#!/bin/bash

set -ex

ParDir=$(basename `pwd`)

docker-compose down --remove-orphans

docker volume rm -f ${ParDir}_data ${ParDir}_app-volume

docker build -t spark-base:latest .

docker-compose build && docker-compose up --scale spark-worker=2
