# Hosting ReportsDashboard on AWS EC2 (T2 Micro)

This guide provides step-by-step instructions to host your dashboard on an AWS EC2 instance.

## 1. Launch EC2 Instance
- **OS**: Ubuntu 24.04 LTS (Amazon Machine Image)
- **Instance Type**: `t2.micro` (Free Tier eligible)
- **Key Pair**: Create or select an existing `.pem` key.
- **Network Settings**:
  - Allow SSH traffic (Port 22) from your IP.
  - **IMPORTANT**: Allow HTTP traffic (Port 80) from Anywhere (0.0.0.0/0).

## 2. Connect to your Instance
Use SSH to connect to your instance:
```bash
ssh -i "your-key.pem" ubuntu@your-ec2-public-dns
```

## 3. Upload or Clone Code
You can clone your repository directly on the server:
```bash
git clone https://github.com/teslien/ReportsDashboard.git
cd ReportsDashboard
```

## 4. Run the Deployment Script
The script handles the creation of a Virtual Environment to avoid "externally-managed-environment" errors.

Make the script executable and run it:
```bash
chmod +x deploy_aws.sh
./deploy_aws.sh
```

## 5. Post-Deployment & Manual Updates
If you need to install packages manually, you **must** activate the virtual environment first:
```bash
source venv/bin/activate
pip install -r requirements.txt
```
  ```bash
  nano .env
  ```
- **Restart Service**: After updating `.env`, restart the application:
  ```bash
  sudo systemctl restart reportsdashboard
  ```

## Troubleshooting
- **Check Logs**: If the app isn't working, check the Gunicorn/Systemd logs:
  ```bash
  sudo journalctl -u reportsdashboard
  ```
- **Nginx Error**: Check Nginx status:
  ```bash
  sudo systemctl status nginx
  ```
