#!/usr/bin/env python3
"""
News Monitoring for BSE Monitor
Fetches news every 30 minutes using RSS + API with AI deduplication
Sends NEWS ALERTS to Telegram (without sentiment analysis)

This module:
1. Fetches news for monitored stocks every 30 minutes
2. Uses RSS feeds (primary) + NewsData.io API (backup) + AI deduplication
3. Sends NEWS ALERTS to Telegram with links
4. Stores news in database for later sentiment analysis
5. Does NOT perform sentiment analysis (that's on-demand only)
"""

import os
import requests
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

# Import RSS news fetcher
try:
    from rss_news_fetcher import RSSNewsFetcher
    RSS_AVAILABLE = True
except ImportError:
    RSS_AVAILABLE = False
    print("RSS news fetcher not available")

# Import AI news deduplicator
try:
    from ai_news_deduplicator import ai_deduplicate_news_articles
    AI_DEDUPLICATION_AVAILABLE = True
except ImportError:
    AI_DEDUPLICATION_AVAILABLE = False
    print("AI news deduplicator not available")

def fetch_comprehensive_news_for_monitoring(company_name: str) -> Dict:
    """
    Comprehensive news fetching for background monitoring
    Uses RSS feeds + NewsData.io API + AI deduplication
    
    Returns:
        Dict with deduplicated articles and source information
    """
    all_articles = []
    data_sources = []
    debug_info = []
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Starting comprehensive news fetch for {company_name}")
    
    # 1. Fetch from RSS feeds (priority - real-time)
    if RSS_AVAILABLE:
        try:
            rss_fetcher = RSSNewsFetcher()
            rss_result = rss_fetcher.fetch_comprehensive_rss_news(company_name)
            
            if rss_result.get('success'):
                rss_articles = rss_result.get('articles', [])
                if rss_articles:
                    # Mark articles as from RSS
                    for article in rss_articles:
                        article['source_type'] = 'rss'
                        article['fetch_timestamp'] = datetime.now().isoformat()
                    all_articles.extend(rss_articles)
                    data_sources.extend(rss_result.get('data_sources', []))
                    debug_info.append(f"RSS: Found {len(rss_articles)} articles")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: RSS found {len(rss_articles)} articles")
                else:
                    debug_info.append("RSS: No articles found")
            else:
                debug_info.append(f"RSS: Failed - {rss_result.get('error', 'unknown')}")
        except Exception as e:
            debug_info.append(f"RSS: Exception - {str(e)}")
    else:
        debug_info.append("RSS: Not available")
    
    # 2. Fetch from NewsData.io API (backup)
    api_key = os.environ.get('NEWSDATA_API_KEY')
    if api_key:
        try:
            # Initialize API client
            api_client = NewsDataAPIClient(api_key)
            
            # Use optimized search queries
            from sentiment_analysis_service import get_optimized_search_query
            search_queries = get_optimized_search_query(company_name)
            
            for search_query in search_queries:
                api_result = api_client.fetch_stock_news(search_query, size=5)  # Smaller size for monitoring
                
                if api_result.get('success'):
                    api_articles = api_result.get('articles', [])
                    if api_articles:
                        # Mark articles as from API
                        for article in api_articles:
                            article['source_type'] = 'api'
                            article['search_query_used'] = search_query
                            article['fetch_timestamp'] = datetime.now().isoformat()
                        all_articles.extend(api_articles)
                        data_sources.append(f"NewsData.io API ({len(api_articles)} articles via '{search_query}')")
                        debug_info.append(f"API: Found {len(api_articles)} articles with '{search_query}'")
                        break  # Stop after first successful query
            
            if not any('NewsData.io API' in source for source in data_sources):
                data_sources.append("NewsData.io API (no articles found)")
                debug_info.append("API: No articles found with any search query")
                
        except Exception as e:
            debug_info.append(f"API: Exception - {str(e)}")
    else:
        debug_info.append("API: No API key configured")
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Total articles before deduplication: {len(all_articles)}")
    
    # 3. AI-powered deduplication
    if AI_DEDUPLICATION_AVAILABLE and len(all_articles) >= 3:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Using AI deduplication for {len(all_articles)} articles")
        
        try:
            dedup_result = ai_deduplicate_news_articles(all_articles)
            unique_articles = dedup_result.get('deduplicated_articles', [])
            dedup_stats = dedup_result.get('stats', {})
            duplicate_clusters = dedup_result.get('duplicate_clusters', [])
            
            debug_info.append(f"AI Dedup: {dedup_stats.get('original_count', 0)} → {dedup_stats.get('deduplicated_count', 0)} articles")
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: AI deduplication: {dedup_stats.get('original_count', 0)} → {dedup_stats.get('deduplicated_count', 0)} articles")
                if duplicate_clusters:
                    print(f"NEWS: Found {len(duplicate_clusters)} duplicate clusters")
        
        except Exception as e:
            debug_info.append(f"AI Dedup: Exception - {str(e)}")
            # Fallback to simple deduplication
            unique_articles = simple_deduplicate_articles(all_articles)
            dedup_stats = {'method': 'simple_fallback', 'original_count': len(all_articles), 'deduplicated_count': len(unique_articles)}
    else:
        # Simple deduplication fallback
        unique_articles = simple_deduplicate_articles(all_articles)
        dedup_stats = {'method': 'simple_title_matching', 'original_count': len(all_articles), 'deduplicated_count': len(unique_articles)}
        debug_info.append(f"Simple Dedup: {len(all_articles)} → {len(unique_articles)} articles")
    
    return {
        'success': len(unique_articles) > 0,
        'articles': unique_articles,
        'total_articles': len(unique_articles),
        'data_sources': data_sources,
        'deduplication_stats': dedup_stats,
        'debug_info': debug_info,
        'company_name': company_name,
        'fetch_timestamp': datetime.now().isoformat()
    }

def simple_deduplicate_articles(articles: List[Dict]) -> List[Dict]:
    """
    Simple title-based deduplication fallback
    """
    unique_articles = []
    seen_titles = set()
    
    for article in articles:
        title = article.get('title', '').lower().strip()
        title_key = title[:50] if title else str(len(unique_articles))
        
        if title_key not in seen_titles and title:
            seen_titles.add(title_key)
            article['is_clustered'] = False
            article['duplicate_count'] = 0
            unique_articles.append(article)
    
    return unique_articles
    """NewsData.io API client for fetching stock news"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsdata.io/api/1/news"
        self.headers = {'X-ACCESS-KEY': api_key}
        
        # Rate limiting (free plan: 200 requests/day)
        self.request_count = 0
        self.last_request_time = None
        self.min_delay = 2.0  # 2 second delay between requests
    
    def _rate_limit(self):
        """Implement conservative rate limiting"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def fetch_stock_news(self, stock_query: str, size: int = 10) -> Dict:
        """Fetch news for a specific stock"""
        self._rate_limit()
        
        params = {
            'q': stock_query,
            'language': 'en',
            'country': 'in',
            'category': 'business',
            'size': min(size, 10)  # Free plan limit
        }
        
        try:
            response = requests.get(
                self.base_url, 
                params=params,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'articles': data.get('results', []),
                    'total_results': data.get('totalResults', 0),
                    'query': stock_query,
                    'timestamp': datetime.now().isoformat(),
                    'request_count': self.request_count
                }
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'query': stock_query
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'query': stock_query
            }

def check_news_already_sent(user_client, article: Dict, stock_query: str) -> bool:
    """
    Check if news article (or similar clustered article) has already been sent to users
    Returns True if already sent, False if new
    """
    try:
        # Check primary article ID
        article_id = article.get('article_id', '')
        if article_id:
            result = user_client.table('processed_news_articles')\
                .select('id')\
                .eq('article_id', article_id)\
                .eq('stock_query', stock_query)\
                .execute()
            
            if len(result.data) > 0:
                return True
        
        # For clustered articles, check if any of the merged articles were already sent
        if article.get('is_clustered') and article.get('merged_urls'):
            for url in article.get('merged_urls', []):
                if url:
                    # Create a simple ID from URL for checking
                    url_id = hashlib.md5(url.encode()).hexdigest()[:16]
                    result = user_client.table('processed_news_articles')\
                        .select('id')\
                        .eq('article_id', url_id)\
                        .eq('stock_query', stock_query)\
                        .execute()
                    
                    if len(result.data) > 0:
                        return True
        
        # Fallback: check by title similarity
        title = article.get('title', '').strip()
        if title and len(title) > 20:
            title_key = title[:50].lower()
            # Check recent articles with similar titles (last 7 days)
            cutoff_date = datetime.now() - timedelta(days=7)
            
            result = user_client.table('processed_news_articles')\
                .select('title')\
                .eq('stock_query', stock_query)\
                .gte('processed_at', cutoff_date.isoformat())\
                .execute()
            
            for record in result.data:
                existing_title = record.get('title', '').strip()[:50].lower()
                if existing_title and title_key in existing_title or existing_title in title_key:
                    return True
        
        return False
        
    except Exception as e:
        print(f"Error checking news duplication: {e}")
        return False

def store_news_article(user_client, article: Dict, stock_query: str, user_ids: List[str]):
    """
    Store news article in database for future sentiment analysis
    Handles both single articles and clustered articles
    """
    try:
        # Store primary article
        article_id = article.get('article_id', '')
        if not article_id:
            # Generate ID from URL or title if no article_id
            url = article.get('link', article.get('url', ''))
            title = article.get('title', '')
            if url:
                article_id = hashlib.md5(url.encode()).hexdigest()[:16]
            elif title:
                article_id = hashlib.md5(title.encode()).hexdigest()[:16]
            else:
                article_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:16]
        
        # Prepare article data
        article_data = {
            'article_id': article_id,
            'title': article.get('title', ''),
            'url': article.get('link', article.get('url', '')),
            'source_name': article.get('source', article.get('source_name', '')),
            'pub_date': article.get('pubDate', article.get('published_at', '')),
            'stock_query': stock_query,
            'sent_to_users': user_ids,
            'is_clustered': article.get('is_clustered', False),
            'duplicate_count': article.get('duplicate_count', 0),
            'cluster_reason': article.get('cluster_reason', ''),
            'source_type': article.get('source_type', 'unknown')
        }
        
        user_client.table('processed_news_articles').insert(article_data).execute()
        
        # Store clustered articles as well (for future reference)
        if article.get('is_clustered') and article.get('merged_urls'):
            merged_urls = article.get('merged_urls', [])
            merged_sources = article.get('merged_sources', [])
            
            for idx, url in enumerate(merged_urls[1:], 1):  # Skip first URL (already stored)
                if url:
                    clustered_id = hashlib.md5(url.encode()).hexdigest()[:16]
                    source_name = merged_sources[idx] if len(merged_sources) > idx else f"Source {idx+1}"
                    
                    clustered_data = {
                        'article_id': clustered_id,
                        'title': f"[Clustered] {article.get('title', '')}",
                        'url': url,
                        'source_name': source_name,
                        'pub_date': article.get('pubDate', ''),
                        'stock_query': stock_query,
                        'sent_to_users': user_ids,
                        'is_clustered': True,
                        'cluster_parent_id': article_id,
                        'source_type': 'clustered'
                    }
                    
                    try:
                        user_client.table('processed_news_articles').insert(clustered_data).execute()
                    except Exception as cluster_error:
                        print(f"Error storing clustered article: {cluster_error}")
        
    except Exception as e:
        print(f"Error storing news article: {e}")

def format_news_telegram_message(stock_name: str, articles: List[Dict], dedup_stats: Dict = None) -> str:
    """
    Format news alert for Telegram with deduplication information
    """
    if not articles:
        return f"📰 No recent news found for {stock_name}"
    
    # Header with deduplication info
    dedup_info = ""
    if dedup_stats:
        original_count = dedup_stats.get('original_count', 0)
        deduplicated_count = dedup_stats.get('deduplicated_count', 0)
        method = dedup_stats.get('method', 'unknown')
        
        if original_count > deduplicated_count:
            if method == 'ai_gemini':
                dedup_info = f" (🤖 AI deduplicated: {original_count} → {deduplicated_count})"
            else:
                dedup_info = f" (Deduplicated: {original_count} → {deduplicated_count})"
    
    message = f"""📰 NEWS ALERT: {stock_name}
🕐 {datetime.now().strftime('%Y-%m-%d %H:%M IST')}{dedup_info}

📄 {len(articles)} News Article{'s' if len(articles) > 1 else ''} Found:

"""
    
    # List articles (top 5)
    for i, article in enumerate(articles[:5], 1):
        title = article.get('title', 'No title')
        # Truncate long titles
        if len(title) > 70:
            title = title[:70] + '...'
            
        source = article.get('source', 'Unknown')
        url = article.get('link', article.get('url', ''))
        pub_date = article.get('pubDate', '')
        
        # Check if article is clustered (merged)
        is_clustered = article.get('is_clustered', False)
        duplicate_count = article.get('duplicate_count', 0)
        cluster_icon = "🔗" if is_clustered else "📄"
        cluster_info = f" (+{duplicate_count} similar)" if duplicate_count > 0 else ""
        
        # Format article entry
        message += f"{i}. {cluster_icon} {title}{cluster_info}\n"
        
        # Show primary source or merged sources
        if is_clustered and article.get('merged_sources'):
            sources = article.get('merged_sources', [source])
            if len(sources) > 1:
                message += f"   🏢 Sources: {sources[0]} (+{len(sources)-1} more)\n"
            else:
                message += f"   🏢 {sources[0]}\n"
        else:
            message += f"   🏢 {source}"
        
        # Add timestamp if available
        if pub_date:
            try:
                date_obj = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                formatted_date = date_obj.strftime('%m/%d %H:%M')
                message += f" | ⏰ {formatted_date}"
            except:
                pass
        message += "\n"
        
        # Add primary URL
        if url:
            message += f"   🔗 {url}\n"
        
        # Add additional URLs for clustered articles
        if is_clustered and article.get('merged_urls'):
            additional_urls = article.get('merged_urls', [])[1:]  # Skip first URL (already shown)
            for idx, add_url in enumerate(additional_urls[:2], 1):  # Max 2 additional URLs
                if add_url and add_url != url:
                    source_name = article.get('merged_sources', [])[idx] if len(article.get('merged_sources', [])) > idx else f"Source {idx+1}"
                    message += f"   🔗 {source_name}: {add_url}\n"
        
        message += "\n"
    
    # Add footer
    if len(articles) > 5:
        message += f"ℹ️ Showing top 5 of {len(articles)} articles\n\n"
    
    message += "💡 Tip: Use Sentiment Analysis in the app for detailed market impact analysis!"
    
    return message

def send_news_alerts(user_client, user_id: str, monitored_scrips: List[Dict], telegram_recipients: List[Dict]) -> int:
    """
    Main function for news monitoring - sends news alerts using RSS + API + AI deduplication
    
    Args:
        user_client: Supabase client
        user_id: User ID 
        monitored_scrips: List of user's monitored stocks
        telegram_recipients: List of user's Telegram recipients
        
    Returns:
        Number of messages sent
    """
    messages_sent = 0
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Starting enhanced news monitoring for user {user_id} with {len(monitored_scrips)} stocks")
        print(f"NEWS: RSS available: {RSS_AVAILABLE}, AI deduplication available: {AI_DEDUPLICATION_AVAILABLE}")
    
    for scrip in monitored_scrips:
        company_name = scrip.get('company_name', '')
        bse_code = scrip.get('bse_code', '')
        
        if not company_name:
            continue
        
        try:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Processing {company_name}...")
            
            # Fetch comprehensive news using RSS + API + AI deduplication
            news_result = fetch_comprehensive_news_for_monitoring(company_name)
            
            if not news_result.get('success'):
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No news found for {company_name}")
                continue
            
            articles = news_result.get('articles', [])
            dedup_stats = news_result.get('deduplication_stats', {})
            
            if not articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No articles after processing for {company_name}")
                continue
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Found {len(articles)} articles for {company_name} (after deduplication)")
                if dedup_stats:
                    print(f"NEWS: Deduplication: {dedup_stats.get('original_count', 0)} → {dedup_stats.get('deduplicated_count', 0)} ({dedup_stats.get('method', 'unknown')})")
            
            # Filter out already sent articles
            new_articles = []
            for article in articles:
                if not check_news_already_sent(user_client, article, company_name):
                    new_articles.append(article)
            
            if not new_articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No new articles for {company_name} (all already sent)")
                continue
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Found {len(new_articles)} new articles for {company_name}")
            
            # Format and send Telegram message with deduplication info
            message = format_news_telegram_message(company_name, new_articles, dedup_stats)
            
            # Send to all user's Telegram recipients
            for recipient in telegram_recipients:
                chat_id = recipient['chat_id']
                try:
                    telegram_api_url = f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}"
                    response = requests.post(
                        f"{telegram_api_url}/sendMessage",
                        json={
                            'chat_id': chat_id,
                            'text': message,
                            'parse_mode': 'HTML',
                            'disable_web_page_preview': False  # Show link previews
                        },
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        messages_sent += 1
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"NEWS: Sent enhanced news alert for {company_name} to {chat_id}")
                    else:
                        print(f"NEWS: Telegram API error for {chat_id}: {response.text}")
                        
                except Exception as send_error:
                    print(f"NEWS: Error sending to {chat_id}: {send_error}")
            
            # Store articles in database for future sentiment analysis
            for article in new_articles:
                store_news_article(user_client, article, company_name, [user_id])
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Completed enhanced news monitoring for {company_name}")
        
        except Exception as e:
            print(f"NEWS: Error processing {company_name}: {e}")
            import traceback
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                traceback.print_exc()
            continue
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Completed enhanced news monitoring for user {user_id}, sent {messages_sent} messages")
    
    return messages_sent