#!/usr/bin/env python3
"""
Debug script for news fetching and date filtering
"""

import os
import sys
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def debug_news_fetching():
    """Debug news fetching and date filtering"""
    try:
        print("Debugging news fetching and date filtering...")
        
        # Import the RSS news fetcher
        from rss_news_fetcher import RSSNewsFetcher
        from updated_enhanced_news_monitor import EnhancedNewsMonitor
        
        # Test with a known company
        company_name = "Reliance"
        print(f"Testing news fetching for: {company_name}")
        
        # Test RSS fetching
        print("\n=== Testing RSS News Fetcher ===")
        rss_fetcher = RSSNewsFetcher()
        rss_result = rss_fetcher.fetch_comprehensive_rss_news(company_name)
        
        print(f"RSS Fetch Success: {rss_result.get('success')}")
        print(f"Total Articles: {rss_result.get('total_articles')}")
        
        # Show all articles with their dates
        if rss_result.get('articles'):
            print(f"\nAll articles found ({len(rss_result.get('articles', []))}):")
            for i, article in enumerate(rss_result.get('articles', [])[:10]):
                title = article.get('title', 'No title')
                pub_date = article.get('pubDate', article.get('published_at', 'No date'))
                source = article.get('source', 'Unknown')
                print(f"  {i+1}. {title} - {source} - Date: {pub_date}")
        
        # Test date filtering
        print("\n=== Testing Date Filtering ===")
        news_monitor = EnhancedNewsMonitor()
        print(f"Today's date: {news_monitor.today}")
        
        # Check date filtering for each article
        if rss_result.get('articles'):
            today_count = 0
            for article in rss_result.get('articles', [])[:10]:
                pub_date = article.get('pubDate', article.get('published_at', ''))
                is_today = news_monitor.is_today_news(pub_date)
                print(f"  Date: {pub_date} -> Today: {is_today}")
                if is_today:
                    today_count += 1
            
            print(f"\nArticles identified as today's: {today_count}")
        
        return True
        
    except Exception as e:
        print(f"Error debugging news fetching: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_news_fetching()
    sys.exit(0 if success else 1)