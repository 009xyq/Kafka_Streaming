from kafka import KafkaConsumer

brokers = 'ip-10-0-8-55.ec2.internal:9092'
topic = 'xy0615_survery'
# create Kafka consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

va_count = 0
md_count = 0
dc_count = 0

for message in consumer:
    # Parse the message value as a dictionary
    survey = message.value
    if survey['user']['state'] == 'VA':
	    va_count += 1
    elif survey['user']['state'] == 'MD':
	    md_count += 1
    elif survey['user']['state'] == 'DC':
	    dc_count += 1
    print(f"VA count: {va_count}, MD count: {md_count}, DC count: {dc_count}")