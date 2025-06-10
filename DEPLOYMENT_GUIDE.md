# Galveston Scraper - Server Deployment Guide

## 📋 Prerequisites

- Dokploy account and server access
- Brevo API key
- Email address for notifications
- Docker support on your server

## 🚀 Deployment Steps for Dokploy

### 1. Prepare Environment Variables

Create a `.env` file with the following variables:

```env
# Brevo Email Configuration
BREVO_API_KEY=your_brevo_api_key_here
BREVO_SENDER_EMAIL=noreply@galvestonscraper.com
BREVO_SENDER_NAME=Galveston Scraper
NOTIFICATION_EMAIL=your_notification_email@example.com
NOTIFICATION_NAME=Admin

# Scraper Configuration
SCRAPER_THREADS=3

# Server Configuration
SERVER_MODE=production
```

### 2. Deploy on Dokploy

#### Option A: Using Docker Compose (Recommended)

1. **Upload Files to Dokploy:**
   - Upload all project files to your Dokploy server
   - Ensure `docker-compose.yml` and `Dockerfile` are in the root directory

2. **Set Environment Variables in Dokploy Dashboard:**
   - Go to your Dokploy application settings
   - Add the environment variables from step 1
   - **Important:** Replace the placeholder values with your actual:
     - Brevo API key
     - Your notification email address

3. **Deploy:**
   ```bash
   docker-compose up --build -d
   ```

#### Option B: Using Dockerfile Only

1. **Build the Docker image:**
   ```bash
   docker build -t galveston-scraper .
   ```

2. **Run the container:**
   ```bash
   docker run -d \
     --name galveston-tax-scraper \
     -e BREVO_API_KEY="your_brevo_api_key" \
     -e NOTIFICATION_EMAIL="your_email@example.com" \
     -e SCRAPER_THREADS=3 \
     -v $(pwd)/checkpoints:/app/checkpoints \
     -v $(pwd)/logs:/app/logs \
     -v $(pwd)/output:/app/output \
     galveston-scraper
   ```

### 3. Configure Dokploy Application

In your Dokploy dashboard:

1. **Create New Application**
2. **Select Docker Compose or Dockerfile deployment**
3. **Upload project files**
4. **Set environment variables:**
   - `BREVO_API_KEY`: Your Brevo API key
   - `NOTIFICATION_EMAIL`: Your email for notifications
   - `SCRAPER_THREADS`: Number of concurrent workers (default: 3)
   - `BREVO_SENDER_EMAIL`: Sender email address
   - `BREVO_SENDER_NAME`: Sender name

5. **Configure volumes for persistence:**
   - `./checkpoints:/app/checkpoints`
   - `./logs:/app/logs`
   - `./output:/app/output`

### 4. Server Requirements

**Minimum System Requirements:**
- RAM: 4GB (recommended 8GB)
- CPU: 2 cores
- Storage: 10GB free space
- Network: Stable internet connection

**Docker Requirements:**
- Docker Engine 20.10+
- Docker Compose 2.0+

## 🔧 Server-Specific Features

### Headless Browser Operation
- Runs Chrome in headless mode (no GUI)
- Uses Xvfb for virtual display
- Optimized for server environments

### Unique Proxy System
- Each worker thread uses its own proxy (ports 8081, 8082, 8083, etc.)
- Automatic port verification before startup
- Individual proxy lifecycle management

### Error Notifications
- Automatic email alerts for errors
- Detailed error context and stack traces
- Completion notifications with statistics

### Checkpoint System
- Automatic progress saving every few patterns
- Resume capability after interruptions
- Persistent data across container restarts

## 📧 Email Notification Setup

### Getting Brevo API Key

1. Go to [Brevo.com](https://brevo.com)
2. Log in to your account
3. Navigate to **SMTP & API** → **API Keys**
4. Create a new API key
5. Copy the API key for use in environment variables

### Email Templates

The scraper sends two types of emails:

1. **Error Notifications** 🚨
   - Sent when critical errors occur
   - Includes error details and context
   - Helps with debugging and monitoring

2. **Completion Notifications** ✅
   - Sent when scraping completes successfully
   - Includes statistics (records scraped, execution time)
   - Confirms successful operation

## 🔍 Monitoring and Logs

### Accessing Logs
```bash
# View container logs
docker logs galveston-tax-scraper -f

# View log files
tail -f logs/galveston_scraper.log
```

### Health Checks
The container includes health checks to monitor status:
- Checks every 30 seconds
- 3 retry attempts
- 60-second startup period

### Progress Monitoring
- Checkpoint files in `checkpoints/` directory
- CSV outputs in `output/` directory
- Detailed logs in `logs/` directory

## 🛠️ Troubleshooting

### Common Issues

1. **Chrome Driver Issues:**
   - Container automatically installs compatible ChromeDriver
   - Uses webdriver-manager for version management

2. **Memory Issues:**
   - Increase container memory limits
   - Reduce number of threads if needed

3. **Proxy Issues:**
   - Check port availability (8081-8083)
   - Verify proxy configuration

4. **Email Issues:**
   - Verify Brevo API key
   - Check email address validity
   - Review Brevo account limits

### Debug Commands

```bash
# Check container status
docker ps

# Inspect container
docker inspect galveston-tax-scraper

# Check resource usage
docker stats galveston-tax-scraper

# Access container shell
docker exec -it galveston-tax-scraper /bin/bash
```

## 📊 Performance Optimization

### Scaling Options
- Adjust `SCRAPER_THREADS` environment variable
- Monitor system resources
- Consider multiple containers for larger scale

### Resource Tuning
- **Memory:** 2-4GB per container
- **CPU:** 1-2 cores per container
- **Network:** Stable connection for proxy usage

## 🔄 Updates and Maintenance

### Updating the Application
1. Pull new code changes
2. Rebuild Docker image
3. Restart container with new image

### Backup Important Data
- Checkpoint files (for resume capability)
- Log files (for troubleshooting)
- Output CSV files (scraped data)

## 📞 Support

If you encounter issues:
1. Check the logs first
2. Verify environment variables
3. Ensure system requirements are met
4. Monitor email notifications for error details

The scraper is designed to be resilient and will automatically:
- Resume from checkpoints after interruptions
- Send email notifications for monitoring
- Handle errors gracefully
- Manage unique proxies per worker

Happy scraping! 🎯 