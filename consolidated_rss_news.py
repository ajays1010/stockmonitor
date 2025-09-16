#!/usr/bin/env python3
"""
CONSOLIDATED RSS NEWS SYSTEM
Single file containing all RSS news functionality for BSE monitoring system.

Features:
- Advanced filtering and blacklisting
- Company rotation (2 companies per run)
- Clean message formatting (your preferred template)
- Comprehensive duplicate prevention
- Memory efficient processing
- Multiple news sources (RSS + API)

This replaces:
- simple_rss_fix.py
- updated_enhanced_news_monitor.py
- dedicated_rss_news.py
- rss_news_fetcher.py
"""

import os
import requests
import feedparser
import hashlib
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote_plus
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================================================================================
# CONFIGURATION AND CONSTANTS
# ========================================================================================

# Global in-memory cache for duplicate prevention
_RSS_SENT_CACHE = {}
_CACHE_TTL = 3600  # 1 hour

# News source quality filters
QUALITY_SOURCES = [
    'economic times', 'et now', 'economictimes',
    'moneycontrol', 'money control',
    'livemint', 'live mint', 'mint',
    'business standard', 'business today',
    'financial express', 'cnbc tv18', 'cnbctv18',
    'reuters', 'bloomberg', 'ndtv profit',
    'hindu businessline', 'businessline',
    'zeebiz', 'zee business'
]

# Comprehensive blacklist for noise filtering
HEADLINE_BLACKLIST = [
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
    
    # Market movers and generic lists (CRITICAL)
    'gainers', 'losers', 'gainers & losers', 'gainers and losers',
    'top gainers', 'top losers', 'biggest gainers', 'biggest losers',
    'movers', 'big movers', 'top movers', 'market movers',
    'stocks in focus', 'stocks to track', 'stocks in news',
    'buzzing stocks', 'active stocks', 'volume gainers',
    
    # Generic market news and multi-company articles
    'key levels', 'stock market live', 'nifty', 'sensex', 'bse',
    'market today', 'market update', 'live updates', 'market news',
    'shares:', 'stocks:', 'these stocks', 'these shares',
    'midcap stocks', 'smallcap stocks', 'largecap stocks',
    'insurance shareholding', 'mutual fund', 'fii', 'dii',
    'june quarter', 'march quarter', 'december quarter',
    'increased shareholding', 'decreased shareholding',
    
    # Price/volume specific
    'price target', 'target price', 'fair value', 'intrinsic value',
    'book value', 'dividend yield', 'earnings yield', 'pe ratio',
    'price', 'share price', 'stock price', 'trading', 'volume',
    'surge', 'jump', 'fall', 'drop', 'gain', 'loss', 'percent', '%',
    'rupee', 'rs.', 'intraday', 'session', 'market cap',
    'trading session', 'closing price', 'opening price',
    'day high', 'day low', 'week high', 'week low',
    'bull', 'bear', 'rally', 'correction', 'volatility', 'momentum'
]

# ========================================================================================
# COMPANY ROTATION SYSTEM
# ========================================================================================

def get_next_companies_to_process(sb, user_id: str, scrips: List[Dict], batch_size: int = 2) -> List[Dict]:
    """Get the next batch of companies to process using rotation tracking"""
    try:
        # Get last processed company index for this user
        result = sb.table('rss_processing_tracker').select('last_processed_index, updated_at').eq('user_id', user_id).execute()
        
        last_index = 0
        if result.data:
            last_index = result.data[0].get('last_processed_index', 0)
            
            # Check if we completed a full cycle recently (within last hour)
            last_updated = result.data[0].get('updated_at')
            if last_updated:
                try:
                    last_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    if datetime.now().timestamp() - last_time.timestamp() > 3600:  # 1 hour
                        last_index = 0  # Reset if it's been too long
                except:
                    last_index = 0
        
        # Calculate next batch - FIXED LOGIC with duplicate prevention
        start_index = last_index % len(scrips)
        batch = []
        
        # Adjust batch size if user has fewer companies than requested batch size
        effective_batch_size = min(batch_size, len(scrips))
        
        # Get companies without duplicates in the same batch
        for i in range(effective_batch_size):
            company_index = (start_index + i) % len(scrips)
            batch.append(scrips[company_index])
        
        # Calculate next starting index (where next run should start)
        next_index = (start_index + effective_batch_size) % len(scrips)
        
        # Debug information
        batch_indices = [(start_index + i) % len(scrips) for i in range(effective_batch_size)]
        print(f"📰 ROTATION DEBUG: total_companies={len(scrips)}, requested_batch_size={batch_size}, effective_batch_size={effective_batch_size}")
        print(f"📰 ROTATION DEBUG: last_index={last_index}, start_index={start_index}")
        print(f"📰 ROTATION DEBUG: processing indices: {batch_indices}")
        print(f"📰 ROTATION DEBUG: next_index will be: {next_index}")
        
        # Warning if user has fewer companies than batch size
        if effective_batch_size < batch_size:
            print(f"⚠️ USER HAS ONLY {len(scrips)} COMPANIES - using effective batch size {effective_batch_size}")
        
        # Update tracking - with duplicate cleanup
        try:
            from datetime import datetime as dt
            current_time = dt.now().isoformat()
            
            # First, clean up any duplicate entries for this user
            try:
                all_entries = sb.table('rss_processing_tracker').select('id').eq('user_id', user_id).execute()
                if all_entries.data and len(all_entries.data) > 1:
                    print(f"📰 CLEANUP: Found {len(all_entries.data)} duplicate entries for user, cleaning up...")
                    # Keep only the first entry, delete others
                    for entry in all_entries.data[1:]:
                        sb.table('rss_processing_tracker').delete().eq('id', entry['id']).execute()
                    print(f"📰 CLEANUP: Removed {len(all_entries.data) - 1} duplicate entries")
            except Exception as cleanup_error:
                print(f"Warning: Could not cleanup duplicates: {cleanup_error}")
            
            # Now update or insert the tracking record
            if result.data:
                sb.table('rss_processing_tracker').update({
                    'last_processed_index': next_index,
                    'total_companies': len(scrips),
                    'updated_at': current_time
                }).eq('user_id', user_id).execute()
                print(f"📰 TRACKING: Updated user {user_id[:8]} -> next_index={next_index}")
            else:
                sb.table('rss_processing_tracker').insert({
                    'user_id': user_id,
                    'last_processed_index': next_index,
                    'total_companies': len(scrips),
                    'updated_at': current_time
                }).execute()
                print(f"📰 TRACKING: Created new record for user {user_id[:8]} -> next_index={next_index}")
                
        except Exception as e:
            print(f"Warning: Could not update RSS tracking: {e}")
        
        # Debug: Show which companies are being processed
        company_names = [scrip.get('company_name', 'Unknown') for scrip in batch]
        print(f"📰 RSS ROTATION: Processing companies {start_index}-{start_index+len(batch)-1} of {len(scrips)}")
        print(f"📰 COMPANIES IN BATCH: {', '.join(company_names[:3])}{'...' if len(company_names) > 3 else ''}")
        
        return batch
        
    except Exception as e:
        print(f"Warning: RSS tracking failed, using first {batch_size} companies: {e}")
        return scrips[:batch_size]

# ========================================================================================
# DUPLICATE PREVENTION SYSTEM
# ========================================================================================

def generate_article_hash(article: Dict, company_name: str, recipient_id: str) -> str:
    """Generate unique hash for RSS article + recipient combination"""
    title = article.get('title', '')
    url = article.get('link', article.get('url', ''))
    source = article.get('source', article.get('source_name', ''))
    
    # Create unique identifier
    composite = f"{title}|{url}|{company_name}|{recipient_id}|{source}"
    return hashlib.md5(composite.encode('utf-8')).hexdigest()

def is_duplicate_in_memory(article_hash: str) -> bool:
    """Check if article was already processed in memory cache"""
    current_time = time.time()
    
    # Clean expired entries
    expired_keys = [k for k, v in _RSS_SENT_CACHE.items() if current_time - v > _CACHE_TTL]
    for key in expired_keys:
        del _RSS_SENT_CACHE[key]
    
    return article_hash in _RSS_SENT_CACHE

def mark_sent_in_memory(article_hash: str):
    """Mark article as sent in memory cache"""
    _RSS_SENT_CACHE[article_hash] = time.time()

def is_duplicate_in_database(user_client, article: Dict, company_name: str, user_id: str) -> bool:
    """Check if RSS article was already sent using database tracking"""
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
        
        # Check in multiple tables with fallback
        tables_to_check = [
            'news_sent_tracking',
            'processed_news_articles',
            'simple_news_tracking'
        ]
        
        for table_name in tables_to_check:
            try:
                cutoff_date = datetime.now() - timedelta(hours=24)  # 24-hour window
                
                if table_name == 'news_sent_tracking':
                    result = user_client.table(table_name).select('id').eq(
                        'article_id', article_id
                    ).eq('user_id', user_id).eq('company_name', company_name).gte(
                        'sent_at', cutoff_date.isoformat()
                    ).execute()
                elif table_name == 'processed_news_articles':
                    result = user_client.table(table_name).select('id').eq(
                        'article_id', article_id
                    ).eq('stock_query', company_name).gte(
                        'created_at', cutoff_date.isoformat()
                    ).execute()
                else:  # simple_news_tracking
                    article_hash = hashlib.md5(f"{title}_{company_name}".encode()).hexdigest()
                    result = user_client.table(table_name).select('id').eq(
                        'article_hash', article_hash
                    ).eq('user_id', user_id).eq('company_name', company_name).execute()
                
                if result.data:
                    logger.debug(f"RSS duplicate found in {table_name}: {article_id}")
                    return True
                    
            except Exception as e:
                logger.warning(f"Failed to check {table_name}: {e}")
                continue
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking RSS duplicate in database: {e}")
        return False

def record_sent_in_database(user_client, article: Dict, company_name: str, user_id: str):
    """Record RSS article as sent using database tracking"""
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
        
        # Try to record in the most comprehensive table first
        try:
            tracking_data = {
                'article_id': article_id,
                'article_title': title[:500] if title else '',
                'article_url': url[:1000] if url else '',
                'company_name': company_name[:200],
                'user_id': user_id,
                'recipient_id': 'all_recipients',
                'source': 'consolidated_rss',
                'sent_at': datetime.utcnow().isoformat()
            }
            
            user_client.table('news_sent_tracking').insert(tracking_data).execute()
            logger.debug(f"Recorded RSS article in news_sent_tracking: {article_id}")
            
        except Exception as e:
            logger.warning(f"Failed to record in news_sent_tracking: {e}")
            
            # Fallback to processed_news_articles
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
                # Final fallback to simple tracking table
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

# ========================================================================================
# NEWS FILTERING AND RELEVANCE SYSTEM
# ========================================================================================

def is_relevant_news(article: Dict, company_name: str) -> bool:
    """
    Advanced filtering using proven blocklist and relevance checking
    Returns True if relevant, False if should be filtered out
    """
    try:
        title = article.get('title', '').lower()
        description = article.get('description', '').lower()
        content = f"{title} {description}"
        
        # STEP 1: Check headline blacklist (noise filters)
        for blacklisted_phrase in HEADLINE_BLACKLIST:
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

# ========================================================================================
# NEWS FETCHING SYSTEM
# ========================================================================================

def fetch_google_news_rss(company_name: str) -> List[Dict]:
    """Fetch news from Google News RSS for a company with deduplication"""
    try:
        search_queries = [
            f'"{company_name}" India stock news',
            f'"{company_name}" order',
            f'"{company_name}" news',
            f'"{company_name}" results',
            f'"{company_name}" announcement'
        ]
        
        all_articles = []
        seen_articles = set()  # Track duplicates during fetch
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'}
        
        for search_query in search_queries:
            try:
                search_encoded = quote_plus(search_query)
                url = f'https://news.google.com/rss/search?q={search_encoded}&hl=en&gl=IN&ceid=IN:en'
                
                response = requests.get(url, headers=headers, timeout=8)
                if response.status_code != 200:
                    continue
                
                feed = feedparser.parse(response.content)
                
                # Process first 5 entries from each query
                for entry in feed.entries[:5]:
                    title = entry.get('title', '').strip()
                    link = entry.get('link', '').strip()
                    pub_date = entry.get('published', '')
                    
                    if not title or len(title) < 15:
                        continue
                    
                    # Quick relevance check
                    if not is_relevant_news_simple(title, company_name):
                        continue
                    
                    # Deduplicate at source based on title and URL
                    title_clean = title.lower().strip()
                    dedup_key = f"{title_clean}|{link}"
                    
                    if dedup_key in seen_articles:
                        print(f"📰 🚫 SOURCE DUPLICATE: {title[:50]}...")
                        continue
                    
                    seen_articles.add(dedup_key)
                    
                    # Extract source from Google News title format
                    source = 'Google News'
                    if ' - ' in title:
                        parts = title.split(' - ')
                        if len(parts) >= 2:
                            source = parts[-1].strip()
                            title = ' - '.join(parts[:-1]).strip()
                    
                    all_articles.append({
                        'title': title[:150],  # Truncate to save memory
                        'source': source,
                        'link': link,
                        'pubDate': pub_date,
                        'company': company_name,
                        'source_type': 'google_news_rss'
                    })
                    
            except Exception as e:
                print(f"  ❌ Query '{search_query}' failed: {e}")
                continue
        
        print(f"📰 {company_name}: Fetched {len(all_articles)} unique articles (after dedup)")
        return all_articles
        
    except Exception as e:
        print(f"❌ Error in Google News fetch for {company_name}: {e}")
        return []

def is_relevant_news_simple(title: str, company_name: str) -> bool:
    """Simple relevance check for news articles"""
    if not title or not company_name:
        return False
    
    title_lower = title.lower()
    company_lower = company_name.lower()
    
    # Extract company keywords (first word, remove common suffixes)
    company_words = company_lower.replace(' ltd', '').replace(' limited', '').replace(' inc', '').replace(' corp', '').split()
    
    # Check if any company word appears in title
    for word in company_words:
        if len(word) > 3 and word in title_lower:  # Only check meaningful words
            return True
    
    return False

# ========================================================================================
# MESSAGE FORMATTING SYSTEM
# ========================================================================================

def format_clean_rss_message(company_name: str, articles: List[Dict]) -> str:
    """Format RSS news message with your preferred clean layout"""
    from datetime import datetime
    
    if not articles:
        return ""
    
    # Get current date in your preferred format
    current_date = datetime.now().strftime('%B %d, %Y')
    
    # Start with clean header matching your template
    message_parts = [
        "────────────────────",
        "🆕 RSS NEWS",
        f"📰 {company_name} - {current_date}",
        "",
        "📋 Today's Headlines:"
    ]
    
    # Add each article with clean formatting
    for i, article in enumerate(articles, 1):
        title = article.get('title', 'No title available')
        source = article.get('source', article.get('source_name', 'Unknown source'))
        
        # Clean the title (remove redundant company name)
        title_clean = clean_headline_for_display(title, company_name)
        
        # Format: "1. Full headline here"
        message_parts.append(f"{i}. {title_clean}")
        
        # Add source info if available and not generic
        if source and source.lower() not in ['unknown source', 'google news']:
            message_parts.append(f"   📰 {source}")
        
        # Add spacing between articles if multiple
        if i < len(articles):
            message_parts.append("")
    
    return "\n".join(message_parts)

def clean_headline_for_display(title: str, company_name: str) -> str:
    """Clean headline for better display - remove redundant company mentions"""
    try:
        title_clean = title
        
        # Extract company brand name (first word usually)
        company_words = company_name.split()
        if company_words:
            brand_name = company_words[0]
            
            # Remove redundant mentions at the start
            patterns_to_remove = [
                f"{company_name}: ",
                f"{company_name} - ",
                f"{brand_name}: ",
                f"{brand_name} - ",
            ]
            
            for pattern in patterns_to_remove:
                if title_clean.startswith(pattern):
                    title_clean = title_clean[len(pattern):]
                    break
        
        return title_clean.strip()
        
    except Exception:
        return title  # Return original if cleaning fails

# ========================================================================================
# MAIN RSS PROCESSING FUNCTION
# ========================================================================================

def process_consolidated_rss_news(sb, user_id: str, scrips: List[Dict], recipients: List[Dict]) -> int:
    """
    Main function for consolidated RSS news processing
    This is the single entry point that replaces all other RSS functions
    """
    messages_sent = 0
    
    try:
        print(f"🔥 CONSOLIDATED RSS v1.0 - Processing user {user_id[:8]}")
        print(f"🔥 Timestamp: {datetime.now().isoformat()}")
        
        if not scrips or not recipients:
            print("❌ No scrips or recipients found")
            return 0
        
        # Get next batch of companies to process using rotation
        limited_scrips = get_next_companies_to_process(sb, user_id, scrips, batch_size=3)
        
        print(f"📰 CONSOLIDATED RSS: Processing {len(limited_scrips)} companies via rotation")
        
        # Process each company in the batch
        for scrip in limited_scrips:
            company_name = scrip.get('company_name', '')
            if not company_name:
                continue
            
            print(f"📰 Processing company: {company_name}")
            
            try:
                # Fetch news for this company
                raw_articles = fetch_google_news_rss(company_name)
                
                if not raw_articles:
                    print(f"📰 No articles found for {company_name}")
                    continue
                
                print(f"📰 Found {len(raw_articles)} raw articles for {company_name}")
                
                # Process recipients separately to prevent cross-contamination
                for recipient in recipients:
                    recipient_id = recipient['chat_id']
                    user_name = recipient.get('user_name', 'User')
                    
                    print(f"📰 Processing recipient: {user_name} ({recipient_id})")
                    
                    # Filter articles for this specific recipient
                    new_articles = []
                    
                    for article in raw_articles:
                        # FILTER 1: Advanced relevance check
                        if not is_relevant_news(article, company_name):
                            title = article.get('title', 'Unknown')[:50]
                            print(f"📰 🚫 FILTERED (irrelevant): {title}...")
                            continue
                        
                        # Generate unique hash for this article + recipient combination
                        article_hash = generate_article_hash(article, company_name, recipient_id)
                        
                        # FILTER 2: Check memory cache (fastest)
                        if is_duplicate_in_memory(article_hash):
                            title = article.get('title', 'Unknown')[:50]
                            print(f"📰 🚫 MEMORY DUPLICATE: {title}...")
                            continue
                        
                        # FILTER 3: Check database for duplicates
                        if is_duplicate_in_database(sb, article, company_name, user_id):
                            title = article.get('title', 'Unknown')[:50]
                            print(f"📰 🚫 DATABASE DUPLICATE: {title}...")
                            # Mark in memory to avoid future database checks
                            mark_sent_in_memory(article_hash)
                            continue
                        
                        # Article passed all filters - it's new and relevant
                        new_articles.append(article)
                        title = article.get('title', 'Unknown')[:50]
                        print(f"📰 ✅ NEW ARTICLE: {title}...")
                    
                    if not new_articles:
                        print(f"📰 No new articles for {user_name} - {company_name}")
                        continue
                    
                    print(f"📰 Sending {len(new_articles)} new articles to {user_name}")
                    
                    # Format message with clean template
                    telegram_message = format_clean_rss_message(company_name, new_articles)
                    
                    # Send message
                    try:
                        from database import send_telegram_message_with_user_name
                        if send_telegram_message_with_user_name(recipient_id, telegram_message, user_name):
                            messages_sent += 1
                            print(f"📰 ✅ SENT to {user_name}: {len(new_articles)} articles")
                            
                            # Mark articles as sent
                            for article in new_articles:
                                # Mark in memory cache
                                article_hash = generate_article_hash(article, company_name, recipient_id)
                                mark_sent_in_memory(article_hash)
                                
                                # Record in database
                                record_sent_in_database(sb, article, company_name, user_id)
                                
                                title = article.get('title', 'Unknown')[:30]
                                print(f"📰 📝 RECORDED: {title}...")
                        else:
                            print(f"📰 ❌ FAILED to send to {user_name}")
                            
                    except Exception as e:
                        print(f"📰 ❌ ERROR sending to {user_name}: {e}")
                
            except Exception as e:
                print(f"❌ Error processing company {company_name}: {e}")
                continue
        
        print(f"📰 CONSOLIDATED RSS: Completed for user {user_id[:8]}: {messages_sent} messages sent")
        
    except Exception as e:
        print(f"❌ CONSOLIDATED RSS ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        import gc
        gc.collect()
    
    return messages_sent

# ========================================================================================
# CACHE MANAGEMENT
# ========================================================================================

def cleanup_rss_cache():
    """Clean up old entries from memory cache"""
    global _RSS_SENT_CACHE
    current_time = time.time()
    
    # Remove expired entries
    expired_keys = [k for k, v in _RSS_SENT_CACHE.items() if current_time - v > _CACHE_TTL]
    for key in expired_keys:
        del _RSS_SENT_CACHE[key]
    
    if len(expired_keys) > 0:
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

# ========================================================================================
# DATABASE SCHEMA (for reference)
# ========================================================================================

RSS_TRACKING_SQL_SCHEMA = """
-- RSS Processing Tracker Table
CREATE TABLE IF NOT EXISTS rss_processing_tracker (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    last_processed_index INTEGER NOT NULL DEFAULT 0,
    total_companies INTEGER DEFAULT 0,
    cycle_completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id)
);

CREATE INDEX IF NOT EXISTS idx_rss_tracker_user ON rss_processing_tracker(user_id);
CREATE INDEX IF NOT EXISTS idx_rss_tracker_updated ON rss_processing_tracker(updated_at);

-- News Sent Tracking Table (primary)
CREATE TABLE IF NOT EXISTS news_sent_tracking (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR(16) NOT NULL,
    article_title TEXT,
    article_url TEXT,
    company_name VARCHAR(200),
    user_id UUID NOT NULL,
    recipient_id VARCHAR(50),
    source VARCHAR(50) DEFAULT 'consolidated_rss',
    sent_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_sent_tracking_lookup ON news_sent_tracking(article_id, user_id, company_name);
CREATE INDEX IF NOT EXISTS idx_news_sent_tracking_date ON news_sent_tracking(sent_at);

-- Simple News Tracking Table (fallback)
CREATE TABLE IF NOT EXISTS simple_news_tracking (
    id SERIAL PRIMARY KEY,
    article_hash VARCHAR(32) NOT NULL,
    user_id UUID NOT NULL,
    company_name VARCHAR(200),
    article_title TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(article_hash, user_id, company_name)
);

CREATE INDEX IF NOT EXISTS idx_simple_news_tracking_lookup ON simple_news_tracking(article_hash, user_id, company_name);
"""

# ========================================================================================
# TEST FUNCTION
# ========================================================================================

def test_consolidated_rss():
    """Test the consolidated RSS system"""
    print("🧪 Testing Consolidated RSS System")
    
    # Test article hash generation
    test_article = {
        'title': 'Test Company Reports Strong Q2 Results',
        'link': 'https://example.com/test-article',
        'source': 'Economic Times'
    }
    
    hash1 = generate_article_hash(test_article, 'Test Company', 'chat123')
    hash2 = generate_article_hash(test_article, 'Test Company', 'chat123')
    hash3 = generate_article_hash(test_article, 'Test Company', 'chat456')
    
    print(f"✅ Same article, same recipient: {hash1 == hash2}")
    print(f"✅ Same article, different recipient: {hash1 != hash3}")
    
    # Test memory cache
    mark_sent_in_memory(hash1)
    is_dup = is_duplicate_in_memory(hash1)
    print(f"✅ Memory cache working: {is_dup}")
    
    # Test relevance filtering
    test_relevant = is_relevant_news(test_article, 'Test Company')
    print(f"✅ Relevance check: {test_relevant}")
    
    # Test message formatting
    test_message = format_clean_rss_message('Test Company', [test_article])
    print(f"✅ Message formatting working: {len(test_message) > 0}")
    
    # Test cache stats
    stats = get_rss_cache_stats()
    print(f"✅ Cache stats: {stats}")
    
    print("🎉 Consolidated RSS test completed!")

# ========================================================================================
# GLOBAL OPTIMIZATION SYSTEM
# ========================================================================================

def process_rss_globally_optimized(sb, all_users_data: Dict) -> int:
    """
    GLOBALLY OPTIMIZED RSS PROCESSING
    Processes unique companies once and distributes to all interested users.
    Replaces per-user processing to eliminate duplicate API calls.
    """
    from collections import defaultdict
    
    total_messages = 0
    batch_size = 3
    
    try:
        print(f"🌍 GLOBAL RSS: Starting optimized processing for {len(all_users_data)} users")
        
        # Step 1: Build global unique company list
        all_companies = set()
        company_to_users = defaultdict(list)
        
        for user_id, user_data in all_users_data.items():
            user_companies = set()
            for scrip in user_data['scrips']:
                company_name = scrip.get('company_name')
                if company_name:
                    all_companies.add(company_name)
                    user_companies.add(company_name)
                    company_to_users[company_name].append(user_id)
            
            print(f"👤 User {user_id[:8]}: {len(user_companies)} companies")
        
        unique_companies = sorted(list(all_companies))
        print(f"🌍 Total unique companies across all users: {len(unique_companies)}")
        
        # Step 2: Get global rotation state
        try:
            result = sb.table('global_rss_rotation').select('id, last_company_index, total_companies, updated_at, last_batch_companies').execute()
            
            global_index = 0
            existing_record = None
            
            if result.data and len(result.data) > 0:
                existing_record = result.data[0]
                global_index = existing_record.get('last_company_index', 0)
                stored_total = existing_record.get('total_companies', 0)
                last_updated = existing_record.get('updated_at')
                
                print(f"📊 CURRENT STATE: index={global_index}, stored_total={stored_total}, actual_total={len(unique_companies)}")
                
                # Reset if it's been too long (1 hour) OR if company count changed significantly
                if last_updated:
                    try:
                        from datetime import datetime as dt
                        last_time = dt.fromisoformat(last_updated.replace('Z', '+00:00'))
                        time_diff = dt.now().timestamp() - last_time.timestamp()
                        
                        # Reset conditions: timeout OR significant company count change
                        if time_diff > 3600:
                            global_index = 0
                            print(f"🔄 Reset global rotation due to timeout ({time_diff/60:.1f} minutes)")
                        elif abs(stored_total - len(unique_companies)) > 2:
                            global_index = 0
                            print(f"🔄 Reset global rotation due to company count change ({stored_total} → {len(unique_companies)})")
                        else:
                            print(f"🔄 Continuing from stored index {global_index} (last run {time_diff/60:.1f} min ago)")
                    except Exception as parse_error:
                        print(f"Warning: Could not parse last_updated time: {parse_error}")
                        # Don't reset on parse error - use stored index
                        print(f"📊 Using stored index {global_index} despite parse error")
            else:
                print("📊 No existing rotation record found - starting fresh")
                
        except Exception as e:
            print(f"Warning: Could not get global rotation state: {e}")
            global_index = 0
            existing_record = None
        
        # Step 3: Calculate next batch with FIXED logic
        start_index = global_index % len(unique_companies)
        batch_companies = []
        
        # Get exactly batch_size companies, wrapping around if necessary
        for i in range(batch_size):
            company_index = (start_index + i) % len(unique_companies)
            batch_companies.append(unique_companies[company_index])
        
        # Calculate next starting index (where next run should start)
        next_index = (start_index + batch_size) % len(unique_companies)
        
        print(f"🔧 BATCH CALCULATION DEBUG:")
        print(f"   - global_index: {global_index}")
        print(f"   - start_index: {start_index}")
        print(f"   - batch_size: {batch_size}")
        print(f"   - total_companies: {len(unique_companies)}")
        print(f"   - calculated next_index: {next_index}")
        print(f"   - batch_companies: {batch_companies}")
        
        print(f"🔄 GLOBAL ROTATION: Processing companies {start_index}-{start_index+len(batch_companies)-1} of {len(unique_companies)}")
        print(f"📊 COMPANIES IN BATCH: {', '.join(batch_companies)}")
        
        # Step 4: Update global rotation state with comprehensive data
        try:
            from datetime import datetime as dt_update
            current_time = dt_update.now().isoformat()
            
            # Prepare update data
            update_data = {
                'last_company_index': next_index,
                'total_companies': len(unique_companies),
                'last_batch_companies': batch_companies,  # Store actual batch processed
                'updated_at': current_time
            }
            
            print(f"📊 UPDATING ROTATION STATE:")
            print(f"   - last_company_index: {global_index} → {next_index}")
            print(f"   - total_companies: {len(unique_companies)}")
            print(f"   - last_batch_companies: {batch_companies}")
            print(f"   - updated_at: {current_time}")
            
            if existing_record and existing_record.get('id'):
                # Update existing record
                record_id = existing_record.get('id')
                update_result = sb.table('global_rss_rotation').update(update_data).eq('id', record_id).execute()
                
                if update_result.data:
                    print(f"✅ SUCCESSFULLY UPDATED global rotation record ID {record_id}")
                    print(f"   New state: index={next_index}, total={len(unique_companies)}")
                    
                    # Verify the update worked by reading it back
                    verify_result = sb.table('global_rss_rotation').select('last_company_index, total_companies').eq('id', record_id).execute()
                    if verify_result.data:
                        verified_index = verify_result.data[0].get('last_company_index')
                        verified_total = verify_result.data[0].get('total_companies')
                        print(f"🔍 VERIFICATION: DB now shows index={verified_index}, total={verified_total}")
                        
                        if verified_index != next_index:
                            print(f"⚠️ WARNING: Update verification failed! Expected {next_index}, got {verified_index}")
                    else:
                        print(f"⚠️ Could not verify update")
                else:
                    print(f"⚠️ Update returned no data - record ID {record_id}")
                    print(f"⚠️ This might indicate the update failed - will try manual reset")
                    
            else:
                # Insert new record (should only happen once)
                insert_result = sb.table('global_rss_rotation').insert(update_data).execute()
                
                if insert_result.data:
                    new_id = insert_result.data[0].get('id', 'unknown')
                    print(f"✅ CREATED new global rotation record ID {new_id}")
                    print(f"   Initial state: index={next_index}, total={len(unique_companies)}")
                else:
                    print(f"⚠️ Insert returned no data")
                    
        except Exception as e:
            print(f"❌ ERROR updating global rotation: {e}")
            print(f"   Data attempted: {update_data}")
            import traceback
            traceback.print_exc()
            # Continue processing without rotation tracking
            print(f"📊 Processing will continue with companies: {', '.join(batch_companies)}")
        
        # Step 5: Fetch news for each company ONCE
        company_news_cache = {}
        
        for company_name in batch_companies:
            print(f"📰 FETCHING: {company_name}")
            
            try:
                # Fetch news once for this company
                raw_articles = fetch_google_news_rss(company_name)
                
                # Filter for relevance
                relevant_articles = []
                for article in raw_articles:
                    if is_relevant_news(article, company_name):
                        relevant_articles.append(article)
                
                company_news_cache[company_name] = relevant_articles
                interested_users = len(company_to_users[company_name])
                
                print(f"📰 {company_name}: {len(raw_articles)} raw → {len(relevant_articles)} relevant → {interested_users} users interested")
                
            except Exception as e:
                print(f"❌ Error fetching {company_name}: {e}")
                company_news_cache[company_name] = []
        
        # Step 6: Distribute cached news to interested users
        for company_name, articles in company_news_cache.items():
            if not articles:
                continue
            
            interested_user_ids = company_to_users[company_name]
            print(f"📤 DISTRIBUTING {company_name}: {len(articles)} articles to {len(interested_user_ids)} users")
            
            for user_id in interested_user_ids:
                user_data = all_users_data[user_id]
                recipients = user_data['recipients']
                
                try:
                    # Process for this specific user
                    user_messages = process_company_for_user_optimized(
                        sb, user_id, company_name, articles, recipients
                    )
                    total_messages += user_messages
                    
                    if user_messages > 0:
                        print(f"📤 {company_name} → User {user_id[:8]}: {user_messages} messages")
                    
                except Exception as e:
                    print(f"❌ Error processing {company_name} for user {user_id[:8]}: {e}")
        
        print(f"🌍 GLOBAL RSS COMPLETED: {total_messages} total messages sent")
        
        return total_messages
        
    except Exception as e:
        print(f"❌ GLOBAL RSS ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 0

def process_company_for_user_optimized(sb, user_id: str, company_name: str, articles: List[Dict], recipients: List[Dict]) -> int:
    """Process cached articles for a specific user with duplicate checking"""
    try:
        messages_sent = 0
        
        # Process each recipient separately
        for recipient in recipients:
            recipient_id = recipient['chat_id']
            user_name = recipient.get('user_name', 'User')
            
            # Filter articles for this specific recipient
            new_articles = []
            
            # Track articles already added to this message to prevent intra-message duplicates
            seen_in_this_message = set()
            
            for article in articles:
                # Generate unique hash for this article + recipient combination
                article_hash = generate_article_hash(article, company_name, recipient_id)
                
                # Check memory cache (fastest)
                if is_duplicate_in_memory(article_hash):
                    continue
                
                # Check database for duplicates
                if is_duplicate_in_database(sb, article, company_name, user_id):
                    mark_sent_in_memory(article_hash)
                    continue
                
                # CRITICAL: Check for duplicates within this message based on title and URL
                article_title = article.get('title', '').strip().lower()
                article_url = article.get('link', '').strip()
                
                # Create a simple dedup key for this message
                dedup_key = f"{article_title}|{article_url}"
                
                if dedup_key in seen_in_this_message:
                    print(f"📰 🚫 INTRA-MESSAGE DUPLICATE: {article_title[:50]}...")
                    continue
                
                # Add to tracking for this message
                seen_in_this_message.add(dedup_key)
                
                # Article is new and relevant
                new_articles.append(article)
            
            if not new_articles:
                continue
            
            # Format and send message
            telegram_message = format_clean_rss_message(company_name, new_articles)
            
            try:
                from database import send_telegram_message_with_user_name
                if send_telegram_message_with_user_name(recipient_id, telegram_message, user_name):
                    messages_sent += 1
                    
                    # Mark articles as sent
                    for article in new_articles:
                        article_hash = generate_article_hash(article, company_name, recipient_id)
                        mark_sent_in_memory(article_hash)
                        record_sent_in_database(sb, article, company_name, user_id)
                    
            except Exception as e:
                print(f"❌ Error sending to {user_name}: {e}")
        
        return messages_sent
        
    except Exception as e:
        print(f"❌ Error in process_company_for_user_optimized: {e}")
        return 0

# ========================================================================================
# GLOBAL RSS DATABASE SCHEMA
# ========================================================================================

GLOBAL_RSS_SQL_SCHEMA = """
-- GLOBAL RSS ROTATION TABLE
CREATE TABLE IF NOT EXISTS global_rss_rotation (
    id SERIAL PRIMARY KEY,
    last_company_index INTEGER NOT NULL DEFAULT 0,
    total_companies INTEGER DEFAULT 0,
    last_batch_companies TEXT[], -- Store last batch for debugging
    updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert initial row (only one row should exist)
INSERT INTO global_rss_rotation (last_company_index, total_companies) 
VALUES (0, 0) 
ON CONFLICT DO NOTHING;

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_global_rss_rotation_updated ON global_rss_rotation(updated_at);

-- Comments
COMMENT ON TABLE global_rss_rotation IS 'Global rotation state for RSS processing to avoid duplicate company fetches';
COMMENT ON COLUMN global_rss_rotation.last_company_index IS 'Index of last processed company in global unique company list';
COMMENT ON COLUMN global_rss_rotation.total_companies IS 'Total number of unique companies across all users';
"""

if __name__ == "__main__":
    test_consolidated_rss()