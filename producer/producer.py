import serial
import time
import json
from kafka import KafkaProducer
from collections import deque
from datetime import datetime

# 🔌 Change COM port
ser = serial.Serial('COM10', 9600)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

window_size = 5
values = deque(maxlen=window_size)

print("🚀 Producer started...")

while True:
    try:
        line = ser.readline().decode().strip()

        if not line:
            continue

        distance = float(line)

        if distance <= 0:
            continue

        values.append(distance)
        avg_distance = sum(values) / len(values)

        if distance < 5:
            status = "VERY_CLOSE"
        elif distance < 15:
            status = "CLOSE"
        else:
            status = "SAFE"

        timestamp = int(time.time() * 1000)
        readable_time = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

        data = {
            "distance": distance,
            "avg_distance": round(avg_distance, 2),
            "status": status,
            "event_time": timestamp,
            
        }

        print(data)

        producer.send('sensor-data', value=data)

    except Exception as e:
        print("Error:", e)