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

def is_relevant_news(article: Dict, company_name: str) -> bool:
    """
    Advanced filtering using proven blocklist from enhanced_news_monitor.py
    Returns True if relevant, False if should be filtered out
    """
    try:
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()
        content = f"{title} {description}"
        
        # COMPREHENSIVE HEADLINE BLACKLIST (from enhanced_news_monitor.py)
        headline_blacklist = [
            # Generic stock movement phrases
            'stock rises', 'stock falls', 'shares up', 'shares down',
            'stock gains', 'stock drops', 'shares gain', 'shares fall',
            'stock jumps', 'stock tumbles', 'shares jump', 'shares tumble',
            'stock surges', 'stock plunges', 'shares surge', 'shares plunge',
            'stock climbs', 'stock slides', 'shares climb', 'shares slide',
            
            # Generic stock lists and recommendations
            '15 stocks', '10 stocks', '5 stocks', '20 stocks', '12 stocks',
            'top picks', 'hot stocks', 'best stocks', 'stocks to buy',
            'stocks to watch', 'stocks to avoid', 'penny stocks',
            'multibagger', 'multibagger stocks', 'wealth creators',
            'stock picks', 'stock ideas', 'stock tips', 'investment tips',
            'trading tips', 'market tips', 'stock alert', 'buy now', 'sell now',
            
            # Technical analysis noise
            'market volatility', 'technical analysis', 'chart pattern',
            'support level', 'resistance level', 'moving average',
            'fibonacci', 'bollinger bands', 'rsi', 'macd',
            'breakout', 'breakdown', 'trend analysis',
            
            # Generic market commentary
            'market wrap', 'market close', 'market open', 'market update',
            'market buzz', 'market mood', 'market trends', 'market view',
            'weekly roundup', 'daily roundup', 'market roundup',
            'closing bell', 'opening bell', 'pre-market', 'after-market',
            
            # Generic recommendations and lists
            'buy recommendation', 'sell recommendation', 'hold recommendation',
            'analyst recommendation', 'broker recommendation',
            'stock recommendations', 'investment ideas', 'trading ideas',
            'portfolio picks', 'wealth picks', 'investment picks',
            
            # Market movers and generic lists (CRITICAL)
            'gainers', 'losers', 'gainers & losers', 'gainers and losers',
            'top gainers', 'top losers', 'biggest gainers', 'biggest losers',
            'movers', 'big movers', 'top movers', 'market movers',
            'stocks in focus', 'stocks to track', 'stocks in news',
            'buzzing stocks', 'active stocks', 'volume gainers',
            
            # Generic market news and multi-company articles (CRITICAL)
            'key levels', 'stock market live', 'nifty', 'sensex', 'bse',
            'market today', 'market update', 'live updates', 'market news',
            'shares:', 'stocks:', 'these stocks', 'these shares',
            'midcap stocks', 'smallcap stocks', 'largecap stocks',
            'insurance shareholding', 'mutual fund', 'fii', 'dii',
            'june quarter', 'march quarter', 'december quarter',
            'increased shareholding', 'decreased shareholding',
            
            # Price/volume specific (enhanced)
            'price target', 'target price', 'fair value', 'intrinsic value',
            'book value', 'dividend yield', 'earnings yield', 'pe ratio',
            'price', 'share price', 'stock price', 'trading', 'volume',
            'surge', 'jump', 'fall', 'drop', 'gain', 'loss', 'percent', '%',
            'rupee', 'rs.', 'intraday', 'session', 'market cap',
            'trading session', 'closing price', 'opening price',
            'day high', 'day low', 'week high', 'week low',
            'bull', 'bear', 'rally', 'correction', 'volatility', 'momentum'
        ]
        
        # STEP 1: Check headline blacklist (noise filters)
        for blacklisted_phrase in headline_blacklist:
            if blacklisted_phrase in title:
                return False
        
        # STEP 2: Check for generic list articles mentioning multiple companies
        if _is_generic_list_article(title, content, company_name):
            return False
        
        # STEP 3: Check for multiple companies in title
        if _has_multiple_companies_in_title(title, company_name):
            return False
        
        # STEP 4: Check company relevance (minimum 2 mentions)
        company_mentions = _count_company_mentions(content, company_name)
        if company_mentions < 2:
            return False
        
        # STEP 5: Check for irrelevant patterns
        irrelevant_patterns = [
            'market outlook', 'economic survey', 'gdp growth', 'inflation',
            'interest rates', 'monetary policy', 'budget', 'government policy',
            'general market', 'overall market', 'broad market', 'market sentiment',
            'global economy', 'world economy', 'economic indicators',
            'market analysis', 'market review', 'weekly wrap', 'daily wrap'
        ]
        
        for pattern in irrelevant_patterns:
            if pattern in content:
                return False
        
        return True
        
    except Exception as e:
        logger.warning(f"Error in relevance check: {e}")
        return True  # If error, assume relevant to be safe

def _count_company_mentions(content: str, company_name: str) -> int:
    """Count how many times the company is mentioned in the content"""
    try:
        content_lower = content.lower()
        company_lower = company_name.lower()
        
        # Count exact company name mentions
        exact_mentions = content_lower.count(company_lower)
        
        # Also count mentions of company keywords and variations
        company_words = company_lower.split()
        if len(company_words) > 1:
            # For multi-word companies, count mentions of key words
            key_word = company_words[0]  # Usually the brand name
            if len(key_word) > 3:  # Avoid very short words
                exact_mentions += content_lower.count(key_word)
        
        return exact_mentions
        
    except Exception:
        return 1  # Default to assuming it's mentioned

def _is_generic_list_article(title: str, content: str, company_name: str) -> bool:
    """Check if this is a generic list article mentioning multiple companies"""
    try:
        list_indicators = [
            'among', 'including', 'here\'s what', 'here is what',
            'top 7', 'top 5', 'top 10', 'top 15', 'top 20',
            '7 stocks', '5 stocks', '10 stocks', '15 stocks',
            'these stocks', 'other stocks', 'stocks like'
        ]
        
        title_lower = title.lower()
        content_lower = content.lower()
        
        # Check if it's a list-type article
        has_list_indicator = any(indicator in title_lower or indicator in content_lower 
                               for indicator in list_indicators)
        
        if has_list_indicator:
            # Count how many other company names are mentioned
            import re
            company_patterns = [
                r'\b\w+\s+ltd\b', r'\b\w+\s+limited\b', 
                r'\b\w+\s+corp\b', r'\b\w+\s+inc\b',
                r'\b\w+\s+bank\b', r'\b\w+\s+motors\b'
            ]
            
            other_companies = 0
            for pattern in company_patterns:
                matches = re.findall(pattern, content_lower)
                other_companies += len(matches)
            
            # If multiple companies mentioned, it's likely a generic list
            if other_companies >= 3:
                return True
        
        return False
        
    except Exception:
        return False

def _has_multiple_companies_in_title(title: str, target_company: str) -> bool:
    """Check if title mentions multiple companies"""
    try:
        # Look for comma-separated company names
        if ',' in title:
            # Count potential company names
            import re
            company_patterns = [
                r'\b[A-Z][a-zA-Z&\s]+(?:Ltd|Limited|Bank|Corp|Inc|Motors|Power|Electric|Industries|Steel|Oil|Gas)\b',
                r'\b[A-Z][a-zA-Z&\s]*\s+&\s+[A-Z][a-zA-Z&\s]*\b',  # Company & Company
                r'\b[A-Z]{2,}\b'  # Acronyms like HDFC, TVS, M&M
            ]
            
            company_count = 0
            for pattern in company_patterns:
                matches = re.findall(pattern, title)
                company_count += len(matches)
            
            # If 3+ companies mentioned, it's a generic list
            if company_count >= 3:
                return True
                
            # Also check for specific patterns like "Company1, Company2, Company3"
            comma_parts = title.split(',')
            if len(comma_parts) >= 3:
                return True
        
        return False
        
    except Exception:
        return False

def format_clean_rss_message(company_name: str, articles: List[Dict]) -> str:
    """Format RSS news message with clean layout and full headlines"""
    from datetime import datetime
    
    # Get current date
    current_date = datetime.now().strftime('%B %d, %Y')
    
    # Start with clean header
    message_parts = [
        f"👤 {company_name}",
        f"📰 {current_date}",
        "",
        "📋 Today's Headlines:"
    ]
    
    # Add each article with full headline
    for i, article in enumerate(articles, 1):
        title = article.get('title', 'No title available')
        source = article.get('source', article.get('source_name', 'Unknown source'))
        
        # Format: "1. Full headline here"
        # If headline is very long, show full text since no link is provided
        message_parts.append(f"{i}. {title}")
        
        # Add source info if available
        if source and source.lower() != 'unknown source':
            message_parts.append(f"   📰 {source}")
        
        # Add spacing between articles if multiple
        if i < len(articles):
            message_parts.append("")
    
    return "\n".join(message_parts)

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
                    title = article.get('title', 'Unknown')[:50]
                    
                    # FILTER 1: Advanced relevance check (blocks price/volume/generic news)
                    if not is_relevant_news(article, company_name):
                        print(f"📰 RSS: 🚫 FILTERED (irrelevant/noise) for {user_name}: {title}...")
                        continue
                    
                    # Generate unique hash for this article + recipient combination
                    article_hash = generate_rss_article_hash(article, company_name, recipient_id)
                    
                    # FILTER 2: Check memory cache (fastest)
                    if is_rss_duplicate_in_memory(article_hash):
                        print(f"📰 RSS: 🚫 MEMORY DUPLICATE for {user_name}: {title}...")
                        continue
                    
                    # FILTER 3: Check database for global duplicates
                    if is_rss_duplicate_in_database(user_client, article, company_name, user_id):
                        print(f"📰 RSS: 🚫 DATABASE DUPLICATE for {user_name}: {title}...")
                        # Mark in memory to avoid future database checks
                        mark_rss_sent_in_memory(article_hash)
                        continue
                    
                    # Article passed all filters - it's new and relevant
                    new_articles.append(article)
                    print(f"📰 RSS: ✅ NEW ARTICLE for {user_name}: {title}...")
                
                if not new_articles:
                    print(f"📰 RSS: No new articles for {user_name} - {company_name}")
                    continue
                
                print(f"📰 RSS: Sending {len(new_articles)} new articles to {user_name}")
                
                # Generate custom formatted message with full headlines
                telegram_message = format_clean_rss_message(company_name, new_articles)
                
                # Send message with clean format
                personalized_message = telegram_message
                
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