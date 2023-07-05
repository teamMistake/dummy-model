import os
import json
from kafka import KafkaConsumer, KafkaProducer

bootstrap = os.environ["KAFKA_BOOTSTRAP_ADDRESS"]
req = os.environ["KAFKA_REQ_TOPIC"]
resp = os.environ["KAFKA_RESP_TOPIC"]
username = os.environ["KAFKA_USERNAME"]
password = os.environ["KAFKA_PASSWORD"]
node_id = os.environ["NODE_ID"]

model_name = os.environ["MODEL_NAME"]

consumer = KafkaConsumer(req, group_id=f'inf-{model_name}',
                         client_id=f'inference-cons-{node_id}',
                         bootstrap_servers=bootstrap,
                         security_protocol="SASL_PLAINTEXT",
                         sasl_mechanism="SCRAM-SHA-512",
                         sasl_plain_username=username,
                         sasl_plain_password=password)
producer = KafkaProducer(bootstrap_servers=bootstrap,
                         client_id=f'inference-prod-{node_id}',
                         security_protocol="SASL_PLAINTEXT",
                         sasl_mechanism="SCRAM-SHA-512",
                         sasl_plain_username=username,
                         sasl_plain_password=password)

for msg in consumer:
    headerList = msg.headers
    model_match = False
    req_id = ''
    for k, v in headerList:
        if k == "target_model":
            if v.decode() == model_name:
                model_match = True
        if k == "req_id":
            req_id = v.decode()
    if not model_match:
        continue

    content = msg.value.decode()

    parsed = json.loads(content)
    req = parsed['req']
    respThink = f'What do you mean by "{req}"?'
    if parsed['stream']:
        # stream
        building = ''
        seq = 0
        for part in respThink.split(" "):
            building += part + ' '
            seq += 1
            producer.send(resp, json.dumps({
                'resp_partial': part + ' ',
                'resp_full': building,
                'eos': False
            }).encode(), headers=[("req_id", req_id.encode()), ("seq_id", str(seq).encode())])
        producer.send(resp, json.dumps({
            'resp_partial': '',
            'resp_full': respThink,
            'eos': True
        }).encode(), headers=[("req_id", req_id.encode()), ("seq_id", str(seq).encode())])
    else:
        # just single
        producer.send(resp, json.dumps({
            'resp_partial': respThink,
            'resp_full': respThink,
            'eos': True
        }).encode(), headers=[("req_id", req_id.encode()), ("seq_id", b'1')])
