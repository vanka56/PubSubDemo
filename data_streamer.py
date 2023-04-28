import json
import requests
import time
import os
import sys
from google.cloud import pubsub_v1
from datetime import datetime
from faker import Faker


def publish_msg(message_str):
	future = publisher.publish(topic_name, data=str(message_str).encode())
	print(message_str)
	print(future.result())

print(sys.argv)

if len(sys.argv) != 3:
    print("""Error: Incorrect number of parameters.

    Usage: python data_streamer.py <project> <payload_type>

        - project: ID of your GCP project
        - payload: text or JSON
""")

    sys.exit()


# Set the name of the Pub/Sub topic
topic_name = "projects/intense-glow-381816/topics/test-topic"

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Define the message to publish

# prev = {}

while True:
	# message = {"name": "Alice", "age": 30, "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
	# message_str = json.dumps(message).encode("utf-8")
	if sys.argv[2].lower() == 'text':
		fake = Faker()
		message_str = fake.text(max_nb_chars=140)
		publish_msg(message_str)
	else:

		cred = os.environ.get("open_sky_cred").split(':')
		auth=(cred[0], cred[1])
		epoch=int(datetime.now().timestamp())
		response = requests.get("https://opensky-network.org/api/flights/all?begin={}&end={}".format(epoch-30, epoch), auth=auth)
		if response.status_code == 200:
			js = json.loads(response.text)
			# message_str = js
			for i in js:
				message_str = i
		
				# Publish the message to the Pub/Sub topic
				publish_msg(message_str)

	time.sleep(30)
