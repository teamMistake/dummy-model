import os
from kafka import KafkaConsumer, KafkaProducer

bootstrap = os.environ["KAFKA_BOOTSTRAP_ADDRESS"]
req = os.environ["KAFKA_REQ_TOPIC"]
resp = os.environ["KAFKA_RESP_TOPIC"]
username = os.environ["KAFKA_USERNAME"]
password = os.environ["KAFKA_PASSWORD"]

consumer = KafkaConsumer(req, 'dumb',
                         bootstrap_servers=bootstrap,
                         security_protocol="SASL_PLAINTEXT",
                         sasl_mechanism="SCRAM-SHA-512",
                         sasl_plain_username=username,
                         sasl_plain_password=password)
producer = KafkaProducer(bootstrap_servers=bootstrap,
                         security_protocol="SASL_PLAINTEXT",
                         sasl_mechanism="SCRAM-SHA-512",
                         sasl_plain_username=username,
                         sasl_plain_password=password)

for msg in consumer:
    headerList = msg.headers()
    print(msg)
    print(headerList)
