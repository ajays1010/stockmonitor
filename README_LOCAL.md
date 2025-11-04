# BSE Monitor - Local Development Setup

This guide will help you set up and run the BSE Monitor application locally for testing and development.

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Git
- A code editor (VS Code recommended)

### 2. Setup Steps

```bash
# Clone the repository (if you haven't already)
git clone <your-repo-url>
cd stockmonitor-main

# Create a virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment configuration
cp .env.example .env

# Edit .env with your API keys and configuration
notepad .env  # Windows
# or
nano .env      # Linux/Mac
```

### 3. Configure Your Environment (.env file)

Edit the `.env` file with your actual API keys:

```env
# Required for basic functionality
SUPABASE_URL=your-supabase-project-url
SUPABASE_SERVICE_KEY=your-supabase-service-role-key
TELEGRAM_BOT_TOKEN=your-telegram-bot-token
CRON_SECRET_KEY=your-secret-key-for-cron-jobs

# Firebase Configuration (copy from Firebase console)
FIREBASE_SERVICE_ACCOUNT_JSON={"type": "service_account", ...}

# Optional but recommended
GEMINI_API_KEY=your-gemini-api-key
NEWSDATA_API_KEY=your-newsdata-api-key
```

### 4. Start the Application

#### Option 1: Use the startup script (Recommended)

**On Linux/Mac:**
```bash
bash run_local.sh
```

**On Windows:**
```cmd
run_local.bat
```

#### Option 2: Start manually

```bash
# Start the main application
python app.py

# In another terminal, start health monitor (optional)
python health_monitor.py
```

### 5. Verify Installation

Open your browser and navigate to:
- **Main App**: http://localhost:5000
- **Health Check**: http://localhost:5000/health
- **Memory Status**: http://localhost:5000/memory-status

## 🧪 Testing Your Local Setup

### Automated Testing

Run the comprehensive test suite:

```bash
python test_local.py
```

This will test:
- Health endpoints
- Memory management
- Cron job functionality
- Database connectivity
- Admin functions

### Manual Testing

#### Test Health Check
```bash
curl http://localhost:5000/health
```

#### Test Memory Optimization
```bash
curl -X POST http://localhost:5000/admin/memory-optimize
```

#### Test Cron Jobs (requires CRON_SECRET_KEY in .env)
```bash
# Test daily summary
curl "http://localhost:5000/cron/daily_summary?key=YOUR_CRON_SECRET_KEY"

# Test BSE announcements
curl "http://localhost:5000/cron/bse_announcements?key=YOUR_CRON_SECRET_KEY"

# Test price spike alerts
curl "http://localhost:5000/cron/price_spike_alerts?key=YOUR_CRON_SECRET_KEY"
```

## 📊 Available Endpoints

### Health & Monitoring
- `GET /health` - Application health status
- `GET /memory-status` - Detailed memory usage
- `POST /admin/memory-optimize` - Force memory cleanup

### Debug & Development
- `GET /debug/cron_logs` - View recent cron job runs
- `GET /debug/cron_auth` - Test cron authentication
- `GET /monitor/cron_status` - Monitor cron job status

### Cron Jobs (Testing)
- `GET /cron/daily_summary?key=YOUR_KEY` - End-of-day price summary
- `GET /cron/bse_announcements?key=YOUR_KEY` - BSE announcements
- `GET /cron/price_spike_alerts?key=YOUR_KEY` - Price spike alerts

## 🔧 Development Features

### Enhanced Logging
- All logs saved to `logs/` directory
- Real-time console output
- Error tracking and memory monitoring

### Auto-Recovery
- Health monitor restarts failed processes
- Automatic memory cleanup
- Process monitoring every 30 seconds

### Performance Monitoring
- Real-time memory usage tracking
- Response time monitoring
- Database connection pooling

## 🛠️ Configuration Options

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Application port | 5000 |
| `FLASK_DEBUG` | Enable debug mode | 1 (local) |
| `YAHOO_VERBOSE` | Verbose Yahoo Finance logs | 1 |
| `BSE_VERBOSE` | Verbose BSE API logs | 1 |
| `DISABLE_AUTO_CLEANUP` | Disable automatic cleanup | false |

### Memory Settings

- **Memory Cleanup**: Runs every 10 seconds
- **Periodic Cleanup**: Runs every 30 minutes
- **Memory Limit**: Warnings at 75%, critical at 90%
- **Auto-Restart**: Triggered on memory issues or crashes

## 🐛 Troubleshooting

### Common Issues

#### 1. Application Won't Start
```bash
# Check dependencies
pip install -r requirements.txt

# Check .env file
ls -la .env

# Check required files
ls -la app.py health_monitor.py database.py
```

#### 2. Database Connection Issues
- Verify `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` in .env
- Check your Supabase project is active
- Ensure service role key has proper permissions

#### 3. Telegram Notifications Not Working
- Verify `TELEGRAM_BOT_TOKEN` in .env
- Ensure bot is running and has proper permissions
- Check if recipients are configured in database

#### 4. Memory Issues
```bash
# Check memory usage
curl http://localhost:5000/memory-status

# Force memory cleanup
curl -X POST http://localhost:5000/admin/memory-optimize
```

#### 5. Cron Jobs Not Working
- Verify `CRON_SECRET_KEY` in .env
- Check endpoints are accessible:
```bash
curl "http://localhost:5000/cron/daily_summary?key=YOUR_KEY"
```

### Debug Mode

Enable detailed logging:
```env
FLASK_DEBUG=1
YAHOO_VERBOSE=1
BSE_VERBOSE=1
```

### Log Files

Check these log files for debugging:
- `logs/app.log` - Main application logs
- `logs/health_monitor.log` - Health monitor logs
- `logs/critical.log` - Critical errors
- `logs/local_startup.log` - Startup logs

## 📱 Development Workflow

### 1. Start Development
```bash
bash run_local.sh
```

### 2. Make Changes
- Edit code files
- Changes auto-reload with Flask debug mode

### 3. Test Changes
```bash
python test_local.py
```

### 4. Check Logs
```bash
tail -f logs/app.log
```

### 5. Stop Application
Press `Ctrl+C` in the terminal

## 🚀 Deploying to Production

When you're ready to deploy:
1. Push changes to GitHub
2. Render will automatically deploy
3. Update production environment variables in Render dashboard

## 📞 Getting Help

If you encounter issues:
1. Check the log files in `logs/` directory
2. Run `python test_local.py` for diagnostics
3. Review this README for common solutions
4. Check the main README.md for additional information

---

**Happy Testing! 🎉**