#!/bin/bash

# Path to Kafka directory and config
KAFKA_DIR=~/kafka
SERVER_PROPERTIES=$KAFKA_DIR/config/server.properties

# Get the public IP address
TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
# PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)

# Fallback to external service if metadata service fails
if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP=$(curl -s https://checkip.amazonaws.com)
    # Remove any newlines from the IP
    PUBLIC_IP=$(echo $PUBLIC_IP | tr -d '\n')
fi

# Verify we got a valid IP
if [ -z "$PUBLIC_IP" ]; then
    echo "$(date) - Failed to retrieve public IP address" >> /tmp/kafka-ip-update.log
    exit 1
fi

echo "$(date) - Detected public IP: $PUBLIC_IP" >> /tmp/kafka-ip-update.log

# Backup current config
cp $SERVER_PROPERTIES ${SERVER_PROPERTIES}.bak

# Update listeners and advertised.listeners
sed -i "s/^#\?listeners=.*/listeners=PLAINTEXT:\/\/0.0.0.0:9092/" $SERVER_PROPERTIES
sed -i "s/^#\?advertised\.listeners=.*/advertised.listeners=PLAINTEXT:\/\/$PUBLIC_IP:9092/" $SERVER_PROPERTIES

echo "$(date) - Updated Kafka configuration with new public IP: $PUBLIC_IP" >> /tmp/kafka-ip-update.log

# Restart Kafka to apply changes
sleep 5
sudo systemctl restart zookeeper
sleep 5
sudo systemctl restart kafka
# $KAFKA_DIR/bin/kafka-server-stop.sh
# sleep 5
# $KAFKA_DIR/bin/zookeeper-server-stop.sh
# sleep 5
# $KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
# sleep 5
# $KAFKA_DIR/bin/kafka-server-start.sh -daemon $SERVER_PROPERTIES

echo "$(date) - Kafka has been restarted with the new configuration" >> /tmp/kafka-ip-update.log