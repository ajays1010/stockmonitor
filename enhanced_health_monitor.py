#!/usr/bin/env python3
"""
Enhanced Health Monitor with BSE Persistent Tracker Health Check
Monitors memory, BSE tracker health, and prevents SIGKILL issues
"""

import os
import sys
import time
import requests
import logging
import psutil
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedHealthMonitor:
    def __init__(self):
        self.app_url = os.environ.get('APP_URL', 'http://localhost:5000')
        self.max_memory_mb = 180  # Critical threshold for Render free plan
        self.warning_memory_mb = 150  # Warning threshold
        self.failure_count = 0
        self.max_failures = 3
        self.last_restart = None
        self.restart_cooldown = 300  # 5 minutes

    def check_app_health(self):
        """Check overall application health"""
        try:
            response = requests.get(f"{self.app_url}/health", timeout=30)
            if response.status_code == 200:
                data = response.json()
                return True, data
            else:
                return False, None
        except requests.exceptions.RequestException as e:
            logger.error(f"Health check failed: {e}")
            return False, None

    def check_bse_tracker_health(self):
        """Check BSE persistent tracker health"""
        try:
            response = requests.get(f"{self.app_url}/debug/bse_dedup_stats", timeout=30)
            if response.status_code == 200:
                data = response.json()
                stats = data.get('stats', {})

                # Check if tracker is working
                active_announcements = stats.get('active_global_announcements', 0)
                file_exists = stats.get('file_exists', False)
                file_size = stats.get('file_size_kb', 0)

                health_status = "healthy"
                issues = []

                if not file_exists:
                    health_status = "error"
                    issues.append("Tracker storage file missing")
                elif file_size == 0:
                    health_status = "warning"
                    issues.append("Tracker storage file empty")

                if active_announcements > 1000:
                    health_status = "warning"
                    issues.append(f"Too many active announcements: {active_announcements}")

                return health_status, stats, issues
            else:
                return "error", None, [f"HTTP {response.status_code}"]

        except requests.exceptions.RequestException as e:
            return "error", None, [f"Request failed: {e}"]
        except Exception as e:
            return "error", None, [f"Unexpected error: {e}"]

    def check_memory_usage(self):
        """Check memory usage with enhanced monitoring"""
        try:
            # Get memory from health endpoint
            response = requests.get(f"{self.app_url}/health", timeout=30)
            if response.status_code == 200:
                data = response.json()
                system_memory = data.get('system', {})
                memory_mb = system_memory.get('used_mb', 0)

                # Determine memory status
                if memory_mb >= self.max_memory_mb:
                    status = "critical"
                elif memory_mb >= self.warning_memory_mb:
                    status = "warning"
                else:
                    status = "normal"

                return status, memory_mb, data
            else:
                return "error", 0, None

        except Exception as e:
            logger.error(f"Memory check failed: {e}")
            return "error", 0, None

    def force_memory_cleanup(self):
        """Force memory cleanup via dedicated endpoint"""
        try:
            response = requests.post(f"{self.app_url}/admin/memory_cleanup", timeout=30)
            if response.status_code == 200:
                logger.info("Memory cleanup triggered successfully")
                return True
            else:
                logger.error(f"Memory cleanup failed with status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Memory cleanup request failed: {e}")
            return False

    def force_bse_tracker_cleanup(self):
        """Force BSE tracker cleanup"""
        try:
            response = requests.post(f"{self.app_url}/admin/bse_dedup_cleanup", timeout=30)
            if response.status_code == 200:
                logger.info("BSE tracker cleanup triggered successfully")
                return True
            else:
                logger.error(f"BSE tracker cleanup failed with status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"BSE tracker cleanup request failed: {e}")
            return False

    def check_recent_logs_for_sigkill(self):
        """Check recent logs for SIGKILL or worker timeout issues"""
        try:
            log_file = 'logs/app.log'
            if not os.path.exists(log_file):
                return False, []

            # Read last 50 lines looking for critical issues
            with open(log_file, 'r') as f:
                lines = f.readlines()[-50:]

            critical_issues = []
            recent_critical = False

            for line in lines:
                if any(keyword in line.upper() for keyword in
                       ['SIGKILL', 'WORKER TIMEOUT', 'CRITICAL', 'OUT OF MEMORY', 'OOM KILL']):
                    critical_issues.append(line.strip())
                    # Check if error is from last 5 minutes
                    try:
                        time_part = line.split(' - ')[0]
                        error_time = datetime.strptime(time_part, '%Y-%m-%d %H:%M:%S,%f')
                        if datetime.now() - error_time < timedelta(minutes=5):
                            recent_critical = True
                    except:
                        pass

            return recent_critical, critical_issues
        except Exception as e:
            logger.error(f"Log check failed: {e}")
            return False, []

    def run_health_check(self):
        """Run comprehensive health check"""
        logger.info("Starting enhanced health check...")

        issues_found = []
        critical_issues = []

        # 1. Basic app health
        app_healthy, health_data = self.check_app_health()
        if not app_healthy:
            issues_found.append("Application health check failed")
            critical_issues.append("App not responding")
        else:
            logger.info("✓ Application health check passed")

        # 2. Memory check
        memory_status, memory_mb, _ = self.check_memory_usage()
        if memory_status == "critical":
            issues_found.append(f"Critical memory usage: {memory_mb:.1f} MB")
            critical_issues.append(f"Memory {memory_mb:.1f} MB > {self.max_memory_mb} MB")

            # Try immediate cleanup
            logger.warning("Critical memory usage detected, triggering cleanup...")
            self.force_memory_cleanup()

        elif memory_status == "warning":
            issues_found.append(f"High memory usage: {memory_mb:.1f} MB")
            logger.warning(f"High memory usage: {memory_mb:.1f} MB")
        else:
            logger.info(f"✓ Memory usage OK: {memory_mb:.1f} MB")

        # 3. BSE Tracker health
        tracker_status, tracker_stats, tracker_issues = self.check_bse_tracker_health()
        if tracker_status == "error":
            issues_found.append("BSE tracker error")
            critical_issues.append("BSE tracker not responding")
        elif tracker_status == "warning":
            issues_found.extend(tracker_issues)
            logger.warning(f"BSE tracker warnings: {tracker_issues}")
        else:
            logger.info("✓ BSE tracker healthy")

        # 4. Check for recent SIGKILL issues
        recent_sigkill, sigkill_issues = self.check_recent_logs_for_sigkill()
        if recent_sigkill:
            issues_found.append("Recent SIGKILL or worker timeout detected")
            critical_issues.extend(sigkill_issues)
            logger.critical("Recent SIGKILL issues found - this may cause duplicates!")

        # 5. BSE Tracker cleanup if needed
        if tracker_stats and tracker_stats.get('total_memory_entries', 0) > 500:
            logger.info("High BSE tracker memory usage, triggering cleanup...")
            self.force_bse_tracker_cleanup()

        # Evaluate overall health
        if critical_issues:
            logger.critical(f"CRITICAL ISSUES FOUND: {critical_issues}")
            self.failure_count += 1

            if self.failure_count >= self.max_failures:
                logger.critical("Maximum failures reached, attempting recovery...")
                self.attempt_recovery()
        elif issues_found:
            logger.warning(f"Issues found: {issues_found}")
            self.failure_count = max(0, self.failure_count - 1)  # Reset slowly
        else:
            logger.info("✓ All health checks passed")
            self.failure_count = 0

        return {
            'app_healthy': app_healthy,
            'memory_status': memory_status,
            'memory_mb': memory_mb,
            'tracker_status': tracker_status,
            'issues_found': issues_found,
            'critical_issues': critical_issues,
            'failure_count': self.failure_count
        }

    def attempt_recovery(self):
        """Attempt to recover from critical issues"""
        try:
            current_time = datetime.now()

            # Check restart cooldown
            if self.last_restart and (current_time - self.last_restart).seconds < self.restart_cooldown:
                logger.warning("Restart cooldown active, skipping recovery attempt")
                return

            logger.critical("Starting recovery procedures...")

            # 1. Force memory cleanup
            self.force_memory_cleanup()
            time.sleep(5)

            # 2. Force BSE tracker cleanup
            self.force_bse_tracker_cleanup()
            time.sleep(5)

            # 3. Check if recovery worked
            memory_status, memory_mb, _ = self.check_memory_usage()
            if memory_status != "critical":
                logger.info("Recovery successful - memory usage reduced")
                self.failure_count = 0
                return

            # 4. If still critical, trigger graceful restart
            logger.critical("Recovery failed, triggering application restart...")
            self.last_restart = current_time

            # In production, this would trigger a container restart
            # For now, we log the need for manual intervention
            logger.critical("MANUAL INTERVENTION REQUIRED: Application needs restart")

        except Exception as e:
            logger.error(f"Recovery attempt failed: {e}")

def main():
    """Main monitoring loop"""
    monitor = EnhancedHealthMonitor()

    logger.info("Enhanced BSE Health Monitor started")

    while True:
        try:
            result = monitor.run_health_check()

            # Log summary
            status = "HEALTHY" if not result['critical_issues'] else "CRITICAL"
            logger.info(f"Health check complete: {status} - Memory: {result['memory_mb']:.1f}MB, "
                       f"Tracker: {result['tracker_status']}, Failures: {result['failure_count']}")

        except Exception as e:
            logger.error(f"Health check failed: {e}")

        # Wait before next check (2 minutes)
        time.sleep(120)

if __name__ == "__main__":
    main()