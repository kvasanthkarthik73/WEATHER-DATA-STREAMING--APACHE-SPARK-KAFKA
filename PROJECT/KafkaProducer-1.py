import csv
import json
import time
from kafka import KafkaProducer

# Set the path to your CSV file here
csv_file_path = "weather_data.csv"
#batch_size = 22

# Set the Kafka bootstrap servers here
bootstrap_servers = ['localhost:9092']

# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Open the CSV file
with open(csv_file_path, newline='') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Format the row as a dictionary
        data = {
            'Formatted Date': row['Formatted Date'],
            'Summary': row['Summary'],
            'Precip Type': row['Precip Type'],
            'Temperature (C)': float(row['Temperature (C)']),
            'Apparent Temperature (C)': float(row['Apparent Temperature (C)']),
            'Humidity': float(row['Humidity']),
            'Wind Speed (km/h)': float(row['Wind Speed (km/h)']),
            'Wind Bearing (degrees)': float(row['Wind Bearing (degrees)']),
            'Visibility (km)': float(row['Visibility (km)']),
            'Loud Cover': int(row['Loud Cover']),
            'Pressure (millibars)': float(row['Pressure (millibars)']),
            'Daily Summary': row['Daily Summary']
        }
        # Send the data to the Kafka topic named 'weather'
        producer.send('weather', value=data)

# Close the producer
producer.close()
