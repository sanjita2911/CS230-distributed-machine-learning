TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)



# Fallback to external service if metadata service fails
if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP=$(curl -s https://checkip.amazonaws.com)
    # Remove any newlines from the IP
    PUBLIC_IP=$(echo $PUBLIC_IP | tr -d '\n')
fi

# Verify we got a valid IP
if [ -z "$PUBLIC_IP" ]; then
    echo "$(date) - Failed to retrieve public IP address" >> /tmp/redis-ip-update.log
    exit 1
fi

echo "$(date) - Detected public IP: $PUBLIC_IP" >> /tmp/redis-ip-update.log

aws ssm put-parameter --name "/redis/public-address" --value "$PUBLIC_IP:6379" --type String --overwrite
echo "$(date) - Kafka has been restarted with the new configuration" >> /tmp/redis-ip-update.log