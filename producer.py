from kafka import KafkaProducer  

# Initialize Kafka producer  

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')  


print('Zdravo')

producer.close()