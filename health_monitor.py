#!/usr/bin/env python3
"""
Health Check and Auto-Recovery Script for BSE Monitor
This script monitors the application health and implements auto-recovery mechanisms
"""

import os
import sys
import time
import requests
import logging
import psutil
from datetime import datetime, timedelta
import json
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/health_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HealthMonitor:
    def __init__(self):
        self.app_url = os.environ.get('APP_URL', 'http://localhost:5000')
        self.health_endpoint = f"{self.app_url}/health"
        self.memory_endpoint = f"{self.app_url}/memory-status"
        self.restart_script = os.environ.get('RESTART_SCRIPT', 'restart_app.sh')
        self.max_memory_percent = 85
        self.max_failures = 3
        self.failure_count = 0
        self.last_restart = None

    def check_app_health(self):
        """Check application health via health endpoint"""
        try:
            response = requests.get(self.health_endpoint, timeout=30)
            if response.status_code == 200:
                health_data = response.json()
                return health_data.get('status') == 'healthy', health_data
            else:
                logger.warning(f"Health check failed with status {response.status_code}")
                return False, None
        except requests.exceptions.RequestException as e:
            logger.error(f"Health check request failed: {e}")
            return False, None

    def check_memory_usage(self):
        """Check memory usage via memory endpoint"""
        try:
            response = requests.get(self.memory_endpoint, timeout=30)
            if response.status_code == 200:
                memory_data = response.json()
                return memory_data.get('system', {}).get('used_percent', 0), memory_data
            else:
                logger.warning(f"Memory check failed with status {response.status_code}")
                return 0, None
        except requests.exceptions.RequestException as e:
            logger.error(f"Memory check request failed: {e}")
            return 0, None

    def check_process_health(self):
        """Check if the main Flask process is running"""
        try:
            # Check if Flask/gunicorn process is running
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if 'python' in cmdline and ('app.py' in cmdline or 'gunicorn' in cmdline):
                        return True, proc.info
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return False, None
        except Exception as e:
            logger.error(f"Process health check failed: {e}")
            return False, None

    def check_logs_for_errors(self):
        """Check recent logs for critical errors"""
        try:
            log_file = 'logs/app.log'
            if not os.path.exists(log_file):
                return False, []

            # Read last 100 lines of log
            with open(log_file, 'r') as f:
                lines = f.readlines()[-100:]

            error_lines = []
            recent_errors = False

            for line in lines:
                if any(keyword in line.upper() for keyword in ['CRITICAL', 'ERROR', 'EXCEPTION', 'SIGKILL']):
                    error_lines.append(line.strip())
                    # Check if error is from last 10 minutes
                    try:
                        time_part = line.split(' - ')[0]
                        error_time = datetime.strptime(time_part, '%Y-%m-%d %H:%M:%S,%f')
                        if datetime.now() - error_time < timedelta(minutes=10):
                            recent_errors = True
                    except:
                        pass

            return recent_errors, error_lines
        except Exception as e:
            logger.error(f"Log check failed: {e}")
            return False, []

    def restart_application(self):
        """Restart the application"""
        try:
            logger.critical("Initiating application restart...")

            # Prevent restart loops
            if self.last_restart and datetime.now() - self.last_restart < timedelta(minutes=5):
                logger.warning("Restart attempted too soon after previous restart")
                return False

            self.last_restart = datetime.now()

            # Method 1: Try restart script if exists
            if os.path.exists(self.restart_script):
                logger.info(f"Using restart script: {self.restart_script}")
                subprocess.run(['bash', self.restart_script], check=True)
                return True

            # Method 2: For Render/deployment platforms, trigger redeploy via API
            render_deploy_hook = os.environ.get('RENDER_DEPLOY_HOOK')
            if render_deploy_hook:
                logger.info("Triggering Render redeploy...")
                response = requests.post(render_deploy_hook, timeout=60)
                if response.status_code == 200:
                    logger.info("Render redeploy triggered successfully")
                    return True

            # Method 3: Kill process and let deployment manager restart it
            logger.info("Terminating process to trigger restart...")
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if 'python' in cmdline and ('app.py' in cmdline or 'gunicorn' in cmdline):
                        proc.terminate()
                        logger.info(f"Terminated process {proc.info['pid']}")
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            logger.error("Could not restart application - no restart method available")
            return False

        except Exception as e:
            logger.error(f"Restart failed: {e}")
            return False

    def force_memory_cleanup(self):
        """Force memory cleanup via admin endpoint"""
        try:
            cleanup_endpoint = f"{self.app_url}/admin/memory-optimize"
            response = requests.post(cleanup_endpoint, timeout=30)
            if response.status_code == 200:
                logger.info("Memory cleanup triggered successfully")
                return True
            else:
                logger.warning(f"Memory cleanup failed with status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Memory cleanup request failed: {e}")
            return False

    def run_health_check(self):
        """Run comprehensive health check"""
        logger.info("Running health check...")

        # 1. Check application health
        app_healthy, health_data = self.check_app_health()
        if not app_healthy:
            logger.error("Application health check failed")
            self.failure_count += 1
        else:
            logger.info("Application health check passed")
            self.failure_count = 0

        # 2. Check memory usage
        memory_percent, memory_data = self.check_memory_usage()
        if memory_percent > self.max_memory_percent:
            logger.warning(f"High memory usage: {memory_percent}%")
            # Try memory cleanup first
            if not self.force_memory_cleanup():
                logger.error("Memory cleanup failed")
                self.failure_count += 1
        else:
            logger.info(f"Memory usage OK: {memory_percent}%")

        # 3. Check process health
        process_healthy, proc_info = self.check_process_health()
        if not process_healthy:
            logger.error("No running application process found")
            self.failure_count += 1
        else:
            logger.info(f"Process OK: PID {proc_info['pid']}")

        # 4. Check for recent errors
        recent_errors, error_lines = self.check_logs_for_errors()
        if recent_errors:
            logger.error(f"Recent critical errors found: {len(error_lines)}")
            for line in error_lines[-5:]:  # Show last 5 errors
                logger.error(f"  {line}")
            self.failure_count += 1

        # 5. Check if restart is needed
        if self.failure_count >= self.max_failures:
            logger.critical(f"Too many failures ({self.failure_count}), initiating restart")
            if self.restart_application():
                self.failure_count = 0
                logger.info("Application restart initiated")
            else:
                logger.error("Restart failed")

        # Log overall status
        logger.info(f"Health check completed - Failures: {self.failure_count}/{self.max_failures}")

        return {
            'timestamp': datetime.now().isoformat(),
            'app_healthy': app_healthy,
            'memory_percent': memory_percent,
            'process_healthy': process_healthy,
            'recent_errors': recent_errors,
            'failure_count': self.failure_count,
            'health_data': health_data,
            'memory_data': memory_data
        }

def main():
    """Main health monitor loop"""
    logger.info("Starting BSE Monitor Health Check Service")

    monitor = HealthMonitor()

    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)

    while True:
        try:
            status = monitor.run_health_check()

            # Log status to file for monitoring
            with open('logs/health_status.json', 'w') as f:
                json.dump(status, f, indent=2)

            # Wait before next check (5 minutes)
            time.sleep(300)

        except KeyboardInterrupt:
            logger.info("Health monitor stopped by user")
            break
        except Exception as e:
            logger.error(f"Health monitor error: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    main()