#!/bin/bash
# deploy.sh - Deploy Lead Mailer to EC2

set -e

EC2_HOST="ubuntu@35.91.174.41"
KEY_FILE="../humancirclesapp/ayushkey.pem"
APP_DIR="/home/ubuntu/lead-mailer"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=30 -o ServerAliveInterval=10 -o ServerAliveCountMax=6"

# Fix permissions on key file
chmod 400 $KEY_FILE

echo "Deploying Lead Mailer to EC2..."

# 1. Create app directory on EC2
echo "Creating directory..."
ssh -i $KEY_FILE $SSH_OPTS $EC2_HOST "mkdir -p $APP_DIR"

# 2. Sync files to EC2 using rsync
echo "Syncing files..."
echo "⚠️  WARNING: This will overwrite the server's .env file with your local .env"

rsync -avz --delete \
    --exclude '__pycache__' \
    --exclude '.git' \
    --exclude 'venv' \
    --exclude '.DS_Store' \
    --exclude 'sent_log.csv' \
    --exclude '.claude' \
    -e "ssh $SSH_OPTS -i $KEY_FILE" \
    ./ $EC2_HOST:$APP_DIR/

# 3. SSH into EC2 and setup
echo "Starting containers on EC2..."
ssh -i $KEY_FILE $SSH_OPTS $EC2_HOST << 'ENDSSH'
cd /home/ubuntu/lead-mailer

# Install Docker if not exists
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    sudo apt-get update
    sudo apt-get install -y docker.io docker-compose
    sudo usermod -aG docker ubuntu
    sudo chmod 666 /var/run/docker.sock
fi

# Stop existing containers
docker-compose down || true

# Build and start
docker-compose up -d --build

echo "Deployment complete!"
docker-compose ps
ENDSSH

echo ""
echo "Lead Mailer deployed!"
echo "URL: http://35.91.174.41:8501"
echo ""
echo "✅ .env file has been synced from local to server"
echo "⚠️  Make sure port 8501 is open in the EC2 security group"
