# Regarding solution

## How to run

1. run bash build.sh
 - This will remove previous docker images
 - Remove old volumes
 - Build new Image
 - Run the docker-compose service which runs the testing setup.


## Architecture for testing env

1. There is spark-master container with 2 spark-workers and spark-submit container which will submit the job.

2. It is possible to scale docker-compose to any number of instances if we set static IPs in docker-compose carefully.

## Core Logic

0. I assume ip:port as single user (For users behind NAT, they have same IP but different port number)

1. First make a new column which states if present time and previous time of hhtp request by particular user belong to same session

2. Then take a cumulative some, this will automatically give different number to different rows in different sessions

3. Then partition and make various queries

4. Lastly just save rough output to data directory


### Problems still trying to figure out relating to testing architecture

0. It is able to run but still few problems with standalone mode

1. There are few errors like rsync error because it can't find spark host name but it does not effect the output.

2. Some warning relating to some spark jars not found but it does not effect the output

3. When trying to run in docker, spark standalone cluster mode fails relay tasks to worker node(while client mode can connect to both workers and successfully complete the job.).
