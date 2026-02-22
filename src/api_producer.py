import time
import json
from kafka import KafkaProducer
from random import uniform, choice

# Cities and weather conditions
cities = ["London", "Paris", "New York"]
weather_conditions = ["sunny", "mist", "light rain", "moderate rain", "overcast clouds"]

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_weather():
    """Send random weather data for each city"""
    for city in cities:
        message = {
            "city": city,
            "timestamp": int(time.time()),
            "temp": round(uniform(-5, 15), 2),
            "humidity": int(uniform(50, 100)),
            "weather": choice(weather_conditions)
        }
        producer.send('raw_api_events', value=message)
        print("Sent:", message)
    producer.flush()

def main():
    while True:
        send_weather()
        time.sleep(60)  # send every 60 seconds

if __name__ == "__main__":
    main()
