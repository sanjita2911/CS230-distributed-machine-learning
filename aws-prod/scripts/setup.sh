#!/bin/bash
# Update system and install dependencies
sudo apt update && sudo apt install -y python3 python3-pip nfs-common

# Define AWS Region dynamically
AWS_REGION="us-east-2"

# Install Python packages (modify as needed)
pip3 install grpcio grpcio-tools kafka-python

# Set up Kafka connection
KAFKA_BROKER="kafka-server-ip:9092"
echo "bootstrap.servers=${KAFKA_BROKER}" | sudo tee /etc/kafka.conf

# Mount EFS in read-only mode
EFS_ID="fs-03c9b59c01e767329"
EFS_MOUNT_POINT="/mnt/efs"
sudo mkdir -p $EFS_MOUNT_POINT
echo "${EFS_ID}.efs.${AWS_REGION}.amazonaws.com:/ $EFS_MOUNT_POINT nfs4 ro,defaults,_netdev 0 0" | sudo tee -a /etc/fstab

# Force EFS mount
sudo mount -t nfs4 ${EFS_ID}.efs.${AWS_REGION}.amazonaws.com:/ ${EFS_MOUNT_POINT} -o ro

# Verify EFS mount
if df -h | grep -q $EFS_MOUNT_POINT; then
    echo "EFS successfully mounted at $EFS_MOUNT_POINT"
else
    echo "EFS mount failed! Check logs."
fi

# Set up gRPC communication with Master Node
MASTER_NODE_IP="master-node-ip"
echo "MASTER_NODE=${MASTER_NODE_IP}" | sudo tee /etc/grpc.conf

# Auto-run worker script (modify path as needed)
cat <<EOF | sudo tee /usr/local/bin/start_worker.sh
#!/bin/bash
python3 /mnt/efs/worker.py --kafka ${KAFKA_BROKER} --grpc ${MASTER_NODE_IP}
EOF

sudo chmod +x /usr/local/bin/start_worker.sh
nohup /usr/local/bin/start_worker.sh > /var/log/worker.log 2>&1 &

echo "Worker script started successfully!"
