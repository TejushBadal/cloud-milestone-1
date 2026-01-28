from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import csv
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

# Set the project_id with your project ID
project_id="cloud-milestone-1-485720"
topic_name = "designLabels";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages to: {topic_path}.")

file_path = os.path.join(os.path.dirname(__file__), 'Labels.csv')

with open(file_path, 'r') as data:

    #converts CSV row --> dict
    csv_reader = csv.DictReader(data)

    record_count = 0
    for row in csv_reader:
        # populate record dict
        record = {}
        for key, value in row.items():

            record[key] = None if value == '' else value

        # Serialize the dictionary to JSON and encode to bytes
        message = json.dumps(record).encode('utf-8')

        # Publish the message to the topic
        try:
            future = publisher.publish(topic_path, message)
            future.result()  # async wait for complete.
            record_count += 1
            print(f"Published record {record_count}: {record}")
        except Exception as e:
            print(f"Failed to publish record: {e}")

print(f"\nTotal records published: {record_count}")