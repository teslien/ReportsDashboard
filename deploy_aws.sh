#!/bin/bash

# AWS EC2 Ubuntu 24.04 Deployment Script for ReportsDashboard
# This script sets up a Python environment, installs dependencies, and configures Gunicorn.

set -e

PROJECT_DIR="ReportsDashboard"
VENV_DIR="venv"

echo "--- Updating System ---"
sudo apt-get update -y
sudo apt-get upgrade -y

echo "--- Installing Python and Dependencies ---"
sudo apt-get install -y python3-pip python3-venv nginx git libpango-1.0-0 libpangoft2-1.0-0 libjpeg-dev libopenjp2-7-dev libffi-dev pkg-config libcairo2-dev

# Clone the repository (User will need to provide their own Git URL if not already cloned)
# git clone https://github.com/teslien/ReportsDashboard.git
# cd $PROJECT_DIR

echo "--- Setting up Virtual Environment ---"
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate

echo "--- Installing Python Requirements ---"
pip install --upgrade pip
pip install -r requirements.txt
pip install gunicorn

echo "--- Configuring Environment Variables ---"
if [ ! -f .env ]; then
    echo "Creating a dummy .env file. PLEASE UPDATE THIS WITH YOUR ACTUAL SECRETS!"
    cp .env.example .env 2>/dev/null || touch .env
fi

echo "--- Setting up Gunicorn Service ---"
# Create a systemd service file for Gunicorn
sudo bash -c "cat > /etc/systemd/system/reportsdashboard.service <<EOF
[Unit]
Description=Gunicorn instance to serve ReportsDashboard
After=network.target

[Service]
User=\$USER
Group=www-data
WorkingDirectory=$(pwd)
Environment=\"PATH=$(pwd)/$VENV_DIR/bin\"
ExecStart=$(pwd)/$VENV_DIR/bin/gunicorn --workers 3 --bind 0.0.0.0:8000 app:app

[Install]
WantedBy=multi-user.target
EOF"

echo "--- Starting Gunicorn Service ---"
sudo systemctl daemon-reload
sudo systemctl start reportsdashboard
sudo systemctl enable reportsdashboard

echo "--- Configuring Nginx (Optional but Recommended) ---"
sudo bash -c "cat > /etc/nginx/sites-available/reportsdashboard <<EOF
server {
    listen 80;
    server_name _;

    location / {
        include proxy_params;
        proxy_pass http://localhost:8000;
    }
}
EOF"

sudo ln -sf /etc/nginx/sites-available/reportsdashboard /etc/nginx/sites-enabled
sudo rm -f /etc/nginx/sites-enabled/default
sudo systemctl restart nginx

echo "--- Deployment Complete! ---"
echo "Your dashboard should be accessible via your EC2 instance's Public IP address."
echo "Note: Ensure Security Group allows Port 80 (HTTP)."
