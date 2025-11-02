# file scriptstest_producer.py
from kafka import KafkaProducer
import json
p = KafkaProducer(bootstrap_servers=['localhost:9092'],
                  value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                  linger_ms=0,
                  request_timeout_ms=10000)
msg = {"test": "hello-from-windows", "ts": __import__('time').time()}
f = p.send('transactions.raw', value=msg)
res = f.get(timeout=10)
print("Sent:", res)
p.flush()
p.close()