#!/usr/bin/env python3
"""
Daily Scheduler for Render Free Plan
Triggers daily summary at 4:00 PM IST using internal timer
"""

import os
import time
import threading
import logging
from datetime import datetime, timedelta
import pytz
import requests

logger = logging.getLogger(__name__)

class DailyScheduler:
    def __init__(self):
        self.base_url = os.environ.get('APP_URL', 'http://localhost:5000')
        self.cron_key = os.environ.get('CRON_SECRET_KEY')
        self.running = False
        self.thread = None

        # Timezone setup (IST)
        self.ist = pytz.timezone('Asia/Kolkata')
        self.target_time = (16, 0)  # 4:00 PM IST

    def get_next_trigger_time(self):
        """Get the next trigger time for 4:00 PM IST"""
        now = datetime.now(self.ist)

        # Get today at 4:00 PM IST
        today_4pm = now.replace(hour=16, minute=0, second=0, microsecond=0)

        # If today is Friday, next trigger is Monday
        if now.weekday() >= 5:  # Friday or weekend
            days_ahead = 7 - now.weekday()  # Days until Monday
            next_trigger = today_4pm + timedelta(days=days_ahead)
        else:
            # If current time is after 4:00 PM, trigger tomorrow
            if now >= today_4pm:
                next_trigger = today_4pm + timedelta(days=1)
            else:
                # Today 4:00 PM
                next_trigger = today_4pm

        return next_trigger

    def start_scheduler(self):
        """Start the daily scheduler"""
        if self.running:
            logger.info("Daily scheduler already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        logger.info("Daily scheduler started")

    def stop_scheduler(self):
        """Stop the daily scheduler"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        logger.info("Daily scheduler stopped")

    def _scheduler_loop(self):
        """Main scheduler loop"""
        logger.info("Daily scheduler loop started")

        while self.running:
            try:
                next_trigger = self.get_next_trigger_time()
                now = datetime.now(self.ist)

                # Calculate seconds to wait
                sleep_seconds = (next_trigger - now).total_seconds()

                if sleep_seconds <= 0:
                    sleep_seconds = 60  # Minimum 1 minute

                logger.info(f"Next daily summary scheduled for: {next_trigger.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                logger.info(f"Waiting {sleep_seconds:.0f} seconds...")

                # Sleep until next trigger time
                time.sleep(min(sleep_seconds, 3600))  # Max 1 hour to prevent long sleeps

                if not self.running:
                    break

                # Check if it's time to trigger
                current_time = datetime.now(self.ist)
                if (current_time.hour == 16 and current_time.minute == 0) or \
                   (current_time >= next_trigger and current_time < next_trigger + timedelta(minutes=1)):

                    self._trigger_daily_summary()

                    # Wait to avoid multiple triggers within the same minute
                    time.sleep(65)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(300)  # Wait 5 minutes on error

    def _trigger_daily_summary(self):
        """Trigger the daily summary cron endpoint"""
        try:
            url = f"{self.base_url}/cron/daily_summary"
            params = {'key': self.cron_key}

            logger.info(f"Triggering daily summary at {datetime.now(self.ist).strftime('%Y-%m-%d %H:%M:%S')}")

            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 200:
                result = response.json()
                logger.info(f"Daily summary triggered successfully: {result}")
            else:
                logger.error(f"Failed to trigger daily summary: HTTP {response.status_code}")

        except Exception as e:
            logger.error(f"Error triggering daily summary: {e}")

# Global scheduler instance
_scheduler = None

def start_daily_scheduler():
    """Start the global daily scheduler"""
    global _scheduler
    if _scheduler is None:
        _scheduler = DailyScheduler()
    _scheduler.start_scheduler()

def stop_daily_scheduler():
    """Stop the global daily scheduler"""
    global _scheduler
    if _scheduler:
        _scheduler.stop_scheduler()
        _scheduler = None

# Auto-start when module is imported
if __name__ == "__main__":
    # Set environment variables for testing
    os.environ['APP_URL'] = os.environ.get('APP_URL', 'http://localhost:5000')
    os.environ['CRON_SECRET_KEY'] = os.environ.get('CRON_SECRET_KEY', 'test-key')

    scheduler = DailyScheduler()

    # Show next trigger time
    next_time = scheduler.get_next_trigger_time()
    print(f"Next daily summary: {next_time.strftime('%Y-%m-%d %H:%M %Z')}")

    # Start scheduler
    scheduler.start_scheduler()

    try:
        # Keep running
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nStopping daily scheduler...")
        scheduler.stop_scheduler()