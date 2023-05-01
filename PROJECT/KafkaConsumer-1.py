from kafka import KafkaConsumer
import json

topic = "weather"

consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

# consume messages from the topic
for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    total_yield = data['TOTAL_YIELD']
    print(f"Received message from topic {topic}: {total_yield}")
    
# close the consumer connection
consumer.close()
