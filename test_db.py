import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port="5433",   # 🔥 THIS IS THE FIX
    database="sensordb",
    user="postgres",
    password="1234"
)

print("CONNECTED SUCCESSFULLY")
conn.close()