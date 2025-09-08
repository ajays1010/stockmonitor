#!/usr/bin/env python3
"""
SIMPLE RSS DUPLICATE FIX - Drop-in Replacement
This is a simple fix you can apply immediately to stop RSS news duplicates.

USAGE:
1. Replace your existing RSS news function call with this one
2. No database changes needed - uses existing tables
3. Works with your current setup

BEFORE:
from updated_enhanced_news_monitor import enhanced_send_news_alerts
sent = enhanced_send_news_alerts(user_client, user_id, monitored_scrips, telegram_recipients)

AFTER:
from simple_rss_fix import send_rss_news_no_duplicates
sent = send_rss_news_no_duplicates(user_client, user_id, monitored_scrips, telegram_recipients)
"""

import hashlib
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global in-memory cache to prevent duplicates within the same process run
_RSS_SENT_CACHE = {}
_CACHE_TTL = 3600  # 1 hour

def generate_rss_article_hash(article: Dict, company_name: str, recipient_id: str) -> str:
    """Generate unique hash for RSS article + recipient combination"""
    # Create a composite string from article details
    title = article.get('title', '')
    url = article.get('link', article.get('url', ''))
    source = article.get('source', article.get('source_name', ''))
    
    # Create unique identifier
    composite = f"{title}|{url}|{company_name}|{recipient_id}|{source}"
    return hashlib.md5(composite.encode('utf-8')).hexdigest()

def is_rss_duplicate_in_memory(article_hash: str) -> bool:
    """Check if article was already processed in memory cache"""
    current_time = time.time()
    
    # Clean expired entries
    expired_keys = [k for k, v in _RSS_SENT_CACHE.items() if current_time - v > _CACHE_TTL]
    for key in expired_keys:
        del _RSS_SENT_CACHE[key]
    
    # Check if already processed
    return article_hash in _RSS_SENT_CACHE

def mark_rss_sent_in_memory(article_hash: str):
    """Mark article as sent in memory cache"""
    _RSS_SENT_CACHE[article_hash] = time.time()

def is_rss_duplicate_in_database(user_client, article: Dict, company_name: str, user_id: str) -> bool:
    """Check if RSS article was already sent using news_sent_tracking table"""
    try:
        # Generate article ID
        url = article.get('link', article.get('url', ''))
        title = article.get('title', '')
        
        if url:
            article_id = hashlib.md5(url.encode()).hexdigest()[:16]
        elif title:
            article_id = hashlib.md5(title.encode()).hexdigest()[:16]
        else:
            return False
        
        # Check in news_sent_tracking table (primary method)
        try:
            cutoff_date = datetime.now() - timedelta(hours=24)  # 24-hour duplicate window
            result = user_client.table('news_sent_tracking').select('id').eq(
                'article_id', article_id
            ).eq('user_id', user_id).eq('company_name', company_name).gte(
                'sent_at', cutoff_date.isoformat()
            ).execute()
            
            if result.data:
                logger.debug(f"RSS duplicate found in news_sent_tracking: {article_id}")
                return True
                
        except Exception as e:
            logger.warning(f"Failed to check news_sent_tracking: {e}")
            
            # Fallback to processed_news_articles table
            try:
                cutoff_date = datetime.now() - timedelta(hours=24)  # 24-hour duplicate window
                result = user_client.table('processed_news_articles').select('id').eq(
                    'article_id', article_id
                ).eq('stock_query', company_name).gte(
                    'created_at', cutoff_date.isoformat()
                ).execute()
                
                if result.data:
                    logger.debug(f"RSS duplicate found in processed_news_articles: {article_id}")
                    return True
                    
            except Exception:
                # Table might not exist or have created_at column, try simple check
                try:
                    result = user_client.table('processed_news_articles').select('id').eq(
                        'article_id', article_id
                    ).eq('stock_query', company_name).execute()
                    
                    if result.data:
                        logger.debug(f"RSS duplicate found (simple check): {article_id}")
                        return True
                except Exception:
                    pass
            
            # Final fallback to simple_news_tracking table
            try:
                article_hash = hashlib.md5(f"{title}_{company_name}".encode()).hexdigest()
                result = user_client.table('simple_news_tracking').select('id').eq(
                    'article_hash', article_hash
                ).eq('user_id', user_id).eq('company_name', company_name).execute()
                
                if result.data:
                    logger.debug(f"RSS duplicate found in simple_news_tracking: {article_hash}")
                    return True
                    
            except Exception:
                pass
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking RSS duplicate in database: {e}")
        return False

def record_rss_sent_in_database(user_client, article: Dict, company_name: str, user_id: str):
    """Record RSS article as sent using news_sent_tracking table"""
    try:
        # Generate article ID
        url = article.get('link', article.get('url', ''))
        title = article.get('title', '')
        
        if url:
            article_id = hashlib.md5(url.encode()).hexdigest()[:16]
        elif title:
            article_id = hashlib.md5(title.encode()).hexdigest()[:16]
        else:
            return
        
        # Record in news_sent_tracking table (primary method)
        try:
            tracking_data = {
                'article_id': article_id,
                'article_title': title[:500] if title else '',
                'article_url': url[:1000] if url else '',
                'company_name': company_name[:200],
                'user_id': user_id,
                'recipient_id': 'all_recipients',  # Will be updated per recipient later
                'source': 'rss',
                'sent_at': datetime.utcnow().isoformat()
            }
            
            user_client.table('news_sent_tracking').insert(tracking_data).execute()
            logger.debug(f"Recorded RSS article in news_sent_tracking: {article_id}")
            
        except Exception as e:
            logger.warning(f"Failed to record in news_sent_tracking: {e}")
            
            # Fallback to processed_news_articles table
            try:
                article_data = {
                    'article_id': article_id,
                    'title': title[:255] if title else '',
                    'url': url[:500] if url else '',
                    'source_name': (article.get('source') or article.get('source_name', ''))[:100],
                    'pub_date': article.get('pubDate', article.get('published_at', ''))[:50],
                    'stock_query': company_name,
                    'sent_to_users': [user_id]
                }
                
                user_client.table('processed_news_articles').insert(article_data).execute()
                logger.debug(f"Recorded RSS article in processed_news_articles: {article_id}")
                
            except Exception:
                # Final fallback to simple_news_tracking table
                try:
                    article_hash = hashlib.md5(f"{title}_{company_name}".encode()).hexdigest()
                    simple_data = {
                        'article_hash': article_hash,
                        'user_id': user_id,
                        'company_name': company_name,
                        'article_title': title[:500] if title else ''
                    }
                    
                    user_client.table('simple_news_tracking').insert(simple_data).execute()
                    logger.debug(f"Recorded RSS article in simple_news_tracking: {article_hash}")
                    
                except Exception as e:
                    logger.warning(f"Could not record RSS article in any table: {e}")
        
    except Exception as e:
        logger.error(f"Error recording RSS article: {e}")

def send_rss_news_no_duplicates(user_client, user_id: str, monitored_scrips: List[Dict], 
                               telegram_recipients: List[Dict]) -> int:
    """
    Send RSS news with comprehensive duplicate prevention
    Drop-in replacement for enhanced_send_news_alerts
    """
    messages_sent = 0
    
    # Add clear logging to identify this system is running
    print(f"🔥 RSS DUPLICATE FIX v1.0 - Processing user {user_id}")
    print(f"🔥 Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Import the news monitor
        from updated_enhanced_news_monitor import EnhancedNewsMonitor
        news_monitor = EnhancedNewsMonitor()
        
        # Process each company
        for scrip in monitored_scrips:
            company_name = scrip.get('company_name', '')
            if not company_name:
                continue
            
            print(f"📰 RSS: Processing {company_name}")
            
            # Fetch today's news
            news_result = news_monitor.fetch_today_news_only(company_name)
            
            if not news_result.get('success'):
                print(f"📰 RSS: No news for {company_name}")
                continue
            
            articles = news_result.get('articles', [])
            if not articles:
                continue
            
            print(f"📰 RSS: Found {len(articles)} articles for {company_name}")
            
            # Process each recipient separately to prevent cross-contamination
            for recipient in telegram_recipients:
                recipient_id = recipient['chat_id']
                user_name = recipient.get('user_name', 'User')
                
                print(f"📰 RSS: Checking articles for {user_name} ({recipient_id})")
                
                # Filter articles for this specific recipient
                new_articles = []
                for article in articles:
                    # Generate unique hash for this article + recipient combination
                    article_hash = generate_rss_article_hash(article, company_name, recipient_id)
                    
                    # Check memory cache first (fastest)
                    if is_rss_duplicate_in_memory(article_hash):
                        title = article.get('title', 'Unknown')[:50]
                        print(f"📰 RSS: 🚫 MEMORY DUPLICATE for {user_name}: {title}...")
                        continue
                    
                    # Check database for global duplicates
                    if is_rss_duplicate_in_database(user_client, article, company_name, user_id):
                        title = article.get('title', 'Unknown')[:50]
                        print(f"📰 RSS: 🚫 DATABASE DUPLICATE for {user_name}: {title}...")
                        # Mark in memory to avoid future database checks
                        mark_rss_sent_in_memory(article_hash)
                        continue
                    
                    # Article is new for this recipient
                    new_articles.append(article)
                    title = article.get('title', 'Unknown')[:50]
                    print(f"📰 RSS: ✅ NEW ARTICLE for {user_name}: {title}...")
                
                if not new_articles:
                    print(f"📰 RSS: No new articles for {user_name} - {company_name}")
                    continue
                
                print(f"📰 RSS: Sending {len(new_articles)} new articles to {user_name}")
                
                # Generate summary and format message
                ai_summary = news_monitor.generate_ai_summary(new_articles, company_name)
                telegram_message = news_monitor.format_crisp_telegram_message(
                    company_name, new_articles, ai_summary
                )
                
                # Send message
                personalized_message = f"🆕 RSS NEWS\n{telegram_message}"
                
                try:
                    from database import send_telegram_message_with_user_name
                    if send_telegram_message_with_user_name(recipient_id, personalized_message, user_name):
                        messages_sent += 1
                        print(f"📰 RSS: ✅ SENT to {user_name}: {len(new_articles)} articles")
                        
                        # Mark articles as sent
                        for article in new_articles:
                            # Mark in memory cache
                            article_hash = generate_rss_article_hash(article, company_name, recipient_id)
                            mark_rss_sent_in_memory(article_hash)
                            
                            # Record in database
                            record_rss_sent_in_database(user_client, article, company_name, user_id)
                            
                            title = article.get('title', 'Unknown')[:30]
                            print(f"📰 RSS: 📝 RECORDED: {title}...")
                    else:
                        print(f"📰 RSS: ❌ FAILED to send to {user_name}")
                        
                except Exception as e:
                    print(f"📰 RSS: ❌ ERROR sending to {user_name}: {e}")
        
        print(f"📰 RSS: Completed for user {user_id}: {messages_sent} messages sent")
        
    except Exception as e:
        print(f"📰 RSS: ❌ ERROR in send_rss_news_no_duplicates: {e}")
        import traceback
        traceback.print_exc()
    
    return messages_sent

def cleanup_rss_cache():
    """Clean up old entries from memory cache"""
    global _RSS_SENT_CACHE
    current_time = time.time()
    
    # Remove expired entries
    expired_keys = [k for k, v in _RSS_SENT_CACHE.items() if current_time - v > _CACHE_TTL]
    for key in expired_keys:
        del _RSS_SENT_CACHE[key]
    
    print(f"📰 RSS: Cleaned up {len(expired_keys)} expired cache entries")

def get_rss_cache_stats() -> Dict:
    """Get statistics about the RSS cache"""
    current_time = time.time()
    active_entries = sum(1 for v in _RSS_SENT_CACHE.values() if current_time - v <= _CACHE_TTL)
    
    return {
        'total_entries': len(_RSS_SENT_CACHE),
        'active_entries': active_entries,
        'expired_entries': len(_RSS_SENT_CACHE) - active_entries,
        'cache_ttl_hours': _CACHE_TTL / 3600
    }

# Test function
def test_rss_fix():
    """Test the RSS duplicate fix"""
    print("🧪 Testing RSS Duplicate Fix")
    
    # Test article hash generation
    test_article = {
        'title': 'Test Company Reports Strong Results',
        'link': 'https://example.com/test',
        'source': 'Economic Times'
    }
    
    hash1 = generate_rss_article_hash(test_article, 'Test Company', 'chat123')
    hash2 = generate_rss_article_hash(test_article, 'Test Company', 'chat123')
    hash3 = generate_rss_article_hash(test_article, 'Test Company', 'chat456')
    
    print(f"✅ Same article, same recipient: {hash1 == hash2}")
    print(f"✅ Same article, different recipient: {hash1 != hash3}")
    
    # Test memory cache
    mark_rss_sent_in_memory(hash1)
    is_dup = is_rss_duplicate_in_memory(hash1)
    print(f"✅ Memory cache working: {is_dup}")
    
    # Test cache stats
    stats = get_rss_cache_stats()
    print(f"✅ Cache stats: {stats}")
    
    print("🎉 RSS fix test completed!")

if __name__ == "__main__":
    test_rss_fix()