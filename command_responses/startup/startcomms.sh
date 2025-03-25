stty -F /dev/ttyUSB1 1200
echo "7E 32 30 30 31 34 36 38 32 43 30 30 34 38 35 32 30 46 43 43 33 0D" /dev/ttyUSB1
stty -F /dev/ttyUSB1 115200
echo "0D 0A" /dev/ttyUSB1

