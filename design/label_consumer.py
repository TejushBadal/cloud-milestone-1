from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="cloud-milestone-1-485720";
topic_name = "designLabels";   # change it for your topic name if needed
subscription_id = "designLabels-sub";   # change it for your topic name if needed

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id,topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # deserialize the message into a dict, as per requirements
    record = json.loads(message.data.decode('utf-8'));
    
    # Print the input vals
    print("=" * 50)
    print("RX'd sensor reading:")
    print(f"  Time: {record.get('time')}")
    print(f"  Profile Name: {record.get('profileName')}")
    print(f"  Temperature: {record.get('temperature')}")
    print(f"  Humidity: {record.get('humidity')}")
    print(f"  Pressure: {record.get('pressure')}")
    print("=" * 50)

    # Acknowledge the message
    message.ack()
    
with subscriber:
    # The call back function will be called for each message recieved from the topic 
    # throught the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("Waiting for messages... (Press Ctrl+C to stop)\n")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nLABEL CONSUMER KILLED")