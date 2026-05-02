import serial

ser = serial.Serial('COM10', 9600)

while True:
    line = ser.readline().decode().strip()
    print(line)