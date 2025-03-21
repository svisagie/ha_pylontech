docker build -t pytes_serial .
docker-compose down
docker-compose up -d
docker logs -f pylon_to_mqtt