#!/usr/bin/env python3
"""
Debug SSL issues in news functionality
"""

import os
import sys
import requests

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def debug_ssl():
    """Debug SSL issues"""
    print("Debugging SSL issues...")
    
    # Test direct requests
    try:
        print("Testing direct requests with SSL verification...")
        response = requests.get("https://httpbin.org/get", verify=True, timeout=10)
        print(f"✅ Direct requests working: {response.status_code}")
    except Exception as e:
        print(f"❌ Direct requests failed: {e}")
        
        # Try without SSL verification
        try:
            print("Testing direct requests without SSL verification...")
            response = requests.get("https://httpbin.org/get", verify=False, timeout=10)
            print(f"✅ Direct requests without SSL verification working: {response.status_code}")
        except Exception as e2:
            print(f"❌ Direct requests without SSL verification also failed: {e2}")
    
    # Test RSS News Fetcher
    try:
        print("Testing RSS News Fetcher...")
        from rss_news_fetcher import RSSNewsFetcher
        fetcher = RSSNewsFetcher()
        
        # Test the _make_request_with_retry method
        print("Testing _make_request_with_retry method...")
        response = fetcher._make_request_with_retry("https://httpbin.org/get", max_retries=1)
        print(f"✅ _make_request_with_retry working: {response.status_code}")
    except Exception as e:
        print(f"❌ RSS News Fetcher test failed: {e}")
        
        # Try to inspect the method
        try:
            import inspect
            source = inspect.getsource(fetcher._make_request_with_retry)
            print("Method source code:")
            print(source)
        except Exception as e2:
            print(f"❌ Failed to inspect method: {e2}")

if __name__ == "__main__":
    debug_ssl()