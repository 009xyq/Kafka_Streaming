import random
from time import sleep
import json
from kafka import KafkaProducer

brokers = ['ip-10-0-8-55.ec2.internal:9092']
topic = 'xy0615_survery'
# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=brokers)

for _ in range(10):
    state = random.choice(["VA", "MD", "DC"])
    zipcode = {"VA": ["22030", "22032", "22033", "22035", "22036"],
           "MD": ["33030", "33032", "33033", "33035", "33036"],
           "DC": ["00030", "00032", "00033", "00035", "00036"]}
    age = ["<18", "18-30", "30-40", ">40"]
    scores_range = range(1,6)

    message_value = {"warehouse":random.choice(scores_range),
                     "datalake": random.choice(scores_range),
                     "lakehouse": random.choice(scores_range),
                     "user": {"state": random.choice(state),
                              "zipcode": random.choice(zipcode.get(state, [])),
                              "age": random.choice(age),
                              "gender": random.choice(["M", "F"])}}
    json_string = json.dumps(message_value)
    producer.send(topic, value=json_string)
    sleep(5)