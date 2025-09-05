#!/usr/bin/env python3
"""
Keep-alive script to prevent Render free tier sleep
"""

import os
import time
import requests
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def ping_app():
    """Ping the application to keep it alive"""
    app_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://multiuser-bse-monitor.onrender.com')
    
    try:
        response = requests.get(f"{app_url}/health", timeout=30)
        if response.status_code == 200:
            logger.info("Successfully pinged app - status: %s", response.status_code)
            return True
        else:
            logger.warning("App ping returned status: %s", response.status_code)
            return False
    except Exception as e:
        logger.error("Error pinging app: %s", str(e))
        return False

def main():
    """Main keep-alive loop"""
    logger.info("Starting keep-alive service")
    
    # Ping every 10 minutes to prevent sleep
    while True:
        try:
            ping_app()
            # Sleep for 10 minutes
            time.sleep(600)
        except KeyboardInterrupt:
            logger.info("Keep-alive service stopped by user")
            break
        except Exception as e:
            logger.error("Unexpected error in keep-alive loop: %s", str(e))
            # Wait a bit before retrying
            time.sleep(60)

if __name__ == "__main__":
    main()