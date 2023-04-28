# PubSubDemo
This is to demonstrate Spark Streaming on GCP infrastructure making use of DataProc and Pub/Sub


# Techstack

Spark Streaming
Scala
DataProc Clusters
Pub/Sub
Basic python

# Concept

There are two parts 

Sender(python) which reads Flight tracking API and puts messages to the Pub/Sub topic

Receiver(spark-streaming) which runs on Dataproc cluster which polls the pub/sub topic and processes the data and presists to Google Storage(GS)


# Pre-requisites

export PROJECT=$(gcloud info --format='value(config.project)')

## Pub/Sub

Create a topic named test_topic and tie it to subscription(test_topic_sub)


## Dataproc

This command prvisions a 4 node spark cluster(1 master / 3 workers, E2-standard) that runs Spark 2.4 version(Bahir streaming libraries dont support the latest ones?)

gcloud dataproc clusters create cluster-7eeb --region us-central1 --zone us-central1-c --master-machine-type e2-standard-2 --master-boot-disk-size 128 --num-workers 3 --worker-machine-type e2-standard-2 --worker-boot-disk-size 128 --image-version 1.5-debian10 --optional-components ANACONDA,JUPYTER --scopes https://www.googleapis.com/auth/compute --project $PROJECT

## Code

cd /PubSubDemo
~/../../PubSubDemo $ mvn clean package

// Following is only required to setup jupyter notebook(spylon kernel) to work with our code
sudo gcloud storage cp gs://$PROJECT/PubSubDemo/my_app-1.0-SNAPSHOT.jar /usr/lib/spark/jars/


## Execution

### Sender

~/../../PubSubDemo cd python_data_streamer
~/../../PubSubDemo/python_data_streamer $ python3 data_streamer.py $PROJECT json

### Receiver

on your local machine(after gcloud setup)

export ARG1="$PROJECT test-topic test-topic-sub json 30 hdfs:///user/spark/checkpoint"
export regionName="us-central1"
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"

~/../../PubSubDemo $ gcloud dataproc jobs submit spark --cluster cluster-7eeb --region $regionName  --jar target/my_app-1.0-SNAPSHOT.jar  --max-failures-per-hour 10 --properties $SPARK_PROPERTIES -- $ARG1

## Monitoring

gcloud dataproc jobs describe <JobID> --region=us-central1 --format='value(status.state)'

or

https://console.cloud.google.com/dataproc/jobs?authuser=1&project=<PROJECTID> (via web browser)








