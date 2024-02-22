from confluent_kafka import Consumer, KafkaError

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic to consume messages from
topic = 'test-topic'

# Create Kafka Consumer configuration
consumer_conf = {'bootstrap.servers': bootstrap_servers, 'group.id': 'my_consumer_group'}

# Create Kafka Consumer instance
consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([topic])

# Consume messages
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    else:
        print(f'Received message: {msg.value().decode("utf-8")}')

# Close the consumer
consumer.close()
