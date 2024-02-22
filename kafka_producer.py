from confluent_kafka import Producer

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Kafka topic to produce messages to
topic = 'test-topic'

# Create Kafka Producer configuration
producer_conf = {'bootstrap.servers': bootstrap_servers}

# Create Kafka Producer instance
producer = Producer(producer_conf)

# Delivery callback function
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic "{msg.topic()}" partition {msg.partition()}')

# Function to send messages
def send_message(message):
    producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)

# Loop to input messages
while True:
    user_input = input("Enter your message (or 'exit' to quit): ")
    if user_input.lower() == 'exit':
        break
    send_message(user_input)

# Wait for all messages to be delivered
producer.flush()

# Close the producer
producer.close()
