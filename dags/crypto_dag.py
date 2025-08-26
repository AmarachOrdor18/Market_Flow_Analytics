import json
import time
import requests
from kafka import KafkaProducer

# kafka setup
kafka_broker = 'localhost:9092'
kafka_topic = 'crypto_prices'

# COINGECKO API setup
coingecko_api_url = 'https://api.coingecko.com/api/v3/coins/markets'
PARAMS = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,litecoin,solana,cardano,ripple,dogecoin,polkadot,binancecoin,chainlink,avalanche,uniswap,polygon,cosmos,stellar,vechain,shiba-inu,tron,tezos,neo',
    'order': 'market_cap_desc',
    'per_page': 20,
    'page': 1,
    'sparkline': False,
    'price_change_percentage': '24h'
}

# desired keys to extract
desired_keys = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume','high_24h', 'low_24h', 'last_updated']

# Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print("Starting to stream crypto prices to Kafka topic...")

try:
    while True:
        response = requests.get(coingecko_api_url, params=PARAMS)
        if response.status_code == 200:
            data = response.json()

            # filter only desired keys from each coin
            filtered_data = [{key: coin.get(key) for key in desired_keys if key in coin} for coin in data]

            payload = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'data': filtered_data
            }

            producer.send(kafka_topic, value=payload)
            print(f"Pushed {len(filtered_data)} records at {payload['timestamp']}")

        else:
            print(f"API Error: {response.status_code} - {response.text}")

        time.sleep(60)  # wait for 1 minute before the next request


except KeyboardInterrupt:
    print("Streaming stopped by user")

finally:
    producer.close()