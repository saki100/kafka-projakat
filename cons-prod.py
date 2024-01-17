from kafka import KafkaConsumer, KafkaProducer
import json, time, requests

# Initialize Kafka consumer

consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='consumer-producer-group')

# Initialize Kafka producer  

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')  

# Weather API configuration  
for message in consumer:
    try:
        # Čitanje podataka iz Kafka poruke
        city = message.value.decode('utf-8')
        print(f"City: {city}")

        # Postavljanje vrednosti za lat i lon u zavisnosti od grada
        if city == 'Beograd':
            lat, lon = 44.787197, 20.457273
        elif city == 'Budimpesta':
            lat, lon = 47.497912, 19.040235
        elif city == 'Prag':
            lat, lon = 50.075538, 14.437800
        elif city == 'Istanbul':
            lat, lon = 41.008240, 28.978359
        elif city == 'Barselona':
            lat, lon = 41.385064, 2.173403
        else:
            print(f"Unknown city: {city}")
            continue

        # Pozivanje Weather API-ja
        api_key = 'adbdd3a5a4ce47b9eaf32860554ca8ae'
        weather_api_url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}'
    
        response = requests.get(weather_api_url)
        
        # Slanje podataka na topic2
        weather_data = json.dumps(response.json())
        producer.send('topic2', value=weather_data.encode('utf-8'))
    
    except Exception as e:
        print(f"Error processing message: {e}")




  

# Send weather data to Kafka topic  

 

# Close the producer  

producer.close() 
consumer.close()