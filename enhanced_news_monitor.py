#!/usr/bin/env python3
"""
Enhanced News Monitor for BSE System
Addresses user feedback:
1. Send only current date news (today's news only)
2. Crisp summaries without URLs
3. Better user experience with focused alerts

Key Improvements:
- Date filtering for today's news only
- AI-powered crisp summaries 
- Compact Telegram messages
- Better relevance filtering
- Sentiment-aware news selection
"""

import os
import requests
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import time

# Import existing components
try:
    from rss_news_fetcher import RSSNewsFetcher
    RSS_AVAILABLE = True
except ImportError:
    RSS_AVAILABLE = False

try:
    from ai_news_deduplicator import ai_deduplicate_news_articles
    AI_DEDUPLICATION_AVAILABLE = True
except ImportError:
    AI_DEDUPLICATION_AVAILABLE = False

def check_news_already_sent(user_client, article: Dict, company_name: str) -> bool:
    """
    Check if news article has already been sent for this company
    Returns True if already sent, False if new
    """
    try:
        # Generate a unique ID for the article based on URL or title
        article_id = article.get('article_id', '')
        if not article_id:
            url = article.get('link', article.get('url', ''))
            title = article.get('title', '')
            if url:
                article_id = hashlib.md5(url.encode()).hexdigest()[:16]
            elif title:
                article_id = hashlib.md5(title.encode()).hexdigest()[:16]
            else:
                article_id = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:16]
        
        # Check if this article has been processed for this company in the last 48 hours
        cutoff_date = datetime.now() - timedelta(hours=48)
        
        result = user_client.table('processed_news_articles')\
            .select('id')\
            .eq('article_id', article_id)\
            .eq('stock_query', company_name)\
            .gte('created_at', cutoff_date.isoformat())\
            .execute()
        
        return len(result.data) > 0
        
    except Exception as e:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Error checking news duplication: {e}")
        return False  # If there's an error, assume it's a new article

def store_sent_news_article(user_client, article: Dict, company_name: str, user_id: str):
    """
    Store information about sent news article to prevent duplicates
    """
    try:
        # Generate a unique ID for the article
        article_id = article.get('article_id', '')
        if not article_id:
            url = article.get('link', article.get('url', ''))
            title = article.get('title', '')
            if url:
                article_id = hashlib.md5(url.encode()).hexdigest()[:16]
            elif title:
                article_id = hashlib.md5(title.encode()).hexdigest()[:16]
            else:
                article_id = hashlib.md5(str(datetime.now()).hexdigest().encode()).hexdigest()[:16]
        
        # Prepare article data for storage
        article_data = {
            'article_id': article_id,
            'title': article.get('title', '')[:255],  # Limit title length
            'url': article.get('link', article.get('url', ''))[:500],  # Limit URL length
            'source_name': article.get('source', article.get('source_name', ''))[:100],  # Limit source name
            'pub_date': article.get('pubDate', article.get('published_at', ''))[:50],  # Limit date string
            'stock_query': company_name,
            'sent_to_users': [user_id],  # Store as array
        }
        
        # Insert into database
        user_client.table('processed_news_articles').insert(article_data).execute()
        
    except Exception as e:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Error storing sent news article: {e}")

class EnhancedNewsMonitor:
    """Enhanced news monitoring with user feedback improvements"""
    
    def __init__(self):
        self.today = datetime.now().date()
        self.ai_api_key = os.environ.get('GOOGLE_API_KEY')
        self.newsdata_api_key = os.environ.get('NEWSDATA_API_KEY')
        
    def is_recent_news(self, pub_date_str: str) -> bool:
        """Check if article is from recent days (rely on database duplicate checking for exact timing)"""
        if not pub_date_str:
            return False  # Exclude articles without dates
            
        try:
            # Parse various date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%d'
            ]
            
            dt_parsed = None
            for fmt in formats:
                try:
                    dt_parsed = datetime.strptime(pub_date_str.strip(), fmt)
                    break
                except ValueError:
                    continue
            
            if dt_parsed is None:
                # Try more lenient parsing as fallback
                try:
                    from dateutil import parser
                    dt_parsed = parser.parse(pub_date_str)
                except:
                    return False
            
            # Handle timezone-aware vs naive datetime comparison
            now = datetime.now()
            if dt_parsed.tzinfo is not None:
                # Article has timezone info, convert to UTC for comparison
                if now.tzinfo is None:
                    # Make now timezone-aware (assume local time)
                    import pytz
                    local_tz = pytz.timezone('Asia/Kolkata')  # IST
                    now = local_tz.localize(now)
                dt_parsed = dt_parsed.astimezone(now.tzinfo)
            else:
                # Both are naive, assume same timezone
                pass
            
            # Check if it's within the last 2 days (let database handle duplicate prevention)
            time_diff = now - dt_parsed
            is_recent = time_diff.total_seconds() <= 2 * 24 * 3600  # 2 days
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Recent check - Article: {pub_date_str} -> {dt_parsed.date()}, Age: {time_diff.total_seconds()/3600:.1f}h, Recent: {is_recent}")
            
            return is_recent
                    
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Recent date parsing error for '{pub_date_str}': {e}")
            return False  # Exclude articles with parsing errors
    
    def is_today_news(self, pub_date_str: str) -> bool:
        """Check if article is from today (rely on database duplicate checking for timing)"""
        if not pub_date_str:
            return False  # Exclude articles without dates
            
        try:
            # Parse various date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%d'
            ]
            
            dt_parsed = None
            for fmt in formats:
                try:
                    dt_parsed = datetime.strptime(pub_date_str.strip(), fmt)
                    break
                except ValueError:
                    continue
            
            if dt_parsed is None:
                # Try more lenient parsing as fallback
                try:
                    from dateutil import parser
                    dt_parsed = parser.parse(pub_date_str)
                except:
                    return False
            
            # Handle timezone-aware vs naive datetime comparison
            now = datetime.now()
            if dt_parsed.tzinfo is not None:
                # Article has timezone info, convert to UTC for comparison
                if now.tzinfo is None:
                    # Make now timezone-aware (assume local time)
                    import pytz
                    local_tz = pytz.timezone('Asia/Kolkata')  # IST
                    now = local_tz.localize(now)
                dt_parsed = dt_parsed.astimezone(now.tzinfo)
            else:
                # Both are naive, assume same timezone
                pass
            
            # Check if it's from today (let database handle duplicate prevention)
            today = now.date()
            article_date = dt_parsed.date()
            is_today = article_date == today
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Date check - Article: {pub_date_str} -> {dt_parsed.date()}, Today: {today}, Is Today: {is_today}")
            
            return is_today
                    
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Date parsing error for '{pub_date_str}': {e}")
            return False  # Exclude articles with parsing errors
    
    def generate_ai_summary(self, articles: List[Dict], company_name: str) -> str:
        """Generate AI-powered crisp summary of today's news"""
        if not self.ai_api_key or not articles:
            return self._generate_simple_summary(articles, company_name)
            
        try:
            # Prepare articles for AI analysis
            articles_text = []
            for article in articles[:5]:  # Use top 5 articles
                title = article.get('title', '')
                description = article.get('description', '')
                source = article.get('source', 'Unknown')
                
                articles_text.append(f"Source: {source}\nTitle: {title}\nDescription: {description}")
            
            combined_text = "\n\n---\n\n".join(articles_text)
            
            # AI prompt for crisp summary
            prompt = f"""
Analyze these news articles about {company_name} and create a crisp 2-3 line summary:

{combined_text}

Requirements:
1. Maximum 2-3 sentences
2. Focus on key developments/events
3. No URLs or links
4. Professional tone
5. Highlight market impact if any

Format: Brief, factual summary suitable for Telegram alert.
"""
            
            # Call Gemini API with proper SSL verification
            response = requests.post(
                f'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key={self.ai_api_key}',
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{
                        'parts': [{'text': prompt}]
                    }]
                },
                timeout=15,
                verify=True  # Enable SSL verification
            )
            
            if response.status_code == 200:
                result = response.json()
                ai_summary = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '').strip()
                
                if ai_summary and len(ai_summary) > 20:
                    return ai_summary
                    
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"AI summary failed: {e}")
        
        # Fallback to simple summary
        return self._generate_simple_summary(articles, company_name)
    
    def _generate_simple_summary(self, articles: List[Dict], company_name: str) -> str:
        """Generate simple summary when AI is not available"""
        if not articles:
            return f"No significant news developments for {company_name} today."
            
        # Extract key themes from titles
        all_titles = [article.get('title', '') for article in articles]
        
        # Simple keyword analysis
        common_keywords = []
        business_keywords = ['profit', 'revenue', 'earnings', 'growth', 'deal', 'merger', 
                           'acquisition', 'launch', 'expansion', 'results', 'agreement', 
                           'partnership', 'investment', 'stake', 'IPO', 'listing']
        
        for keyword in business_keywords:
            count = sum(1 for title in all_titles if keyword.lower() in title.lower())
            if count >= 2:  # Mentioned in multiple articles
                common_keywords.append(keyword)
        
        if common_keywords:
            main_theme = common_keywords[0]
            return f"{company_name} featured in news today regarding {main_theme} and related developments. {len(articles)} news articles covered various aspects."
        else:
            return f"{company_name} appeared in {len(articles)} news articles today covering business developments and market activities."
    
    def fetch_today_news_only(self, company_name: str) -> Dict:
        """Fetch and filter news for today only"""
        all_articles = []
        data_sources = []
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Fetching today's news for {company_name}")
        
        # 1. Fetch from RSS feeds (real-time)
        if RSS_AVAILABLE:
            try:
                rss_fetcher = RSSNewsFetcher()
                rss_result = rss_fetcher.fetch_comprehensive_rss_news(company_name)
                
                if rss_result.get('success'):
                    rss_articles = rss_result.get('articles', [])
                    
                    # Filter for today's articles only
                    today_articles = []
                    for article in rss_articles:
                        pub_date = article.get('pubDate', article.get('published_at', ''))
                        # Only include today's articles
                        if self.is_today_news(pub_date):
                            article['source_type'] = 'rss'
                            today_articles.append(article)
                    
                    all_articles.extend(today_articles)
                    data_sources.append(f"RSS Feeds ({len(today_articles)} today)")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: RSS found {len(today_articles)} articles from today (filtered from {len(rss_articles)} total)")
                        
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: RSS fetch error: {e}")
        
        # 2. Fetch from NewsData.io API (backup, also filter for today)
        if self.newsdata_api_key and len(all_articles) < 5:  # Only if we need more articles
            try:
                # Use today's date for API search
                today_str = self.today.strftime('%Y-%m-%d')
                
                params = {
                    'q': f'"{company_name}"',
                    'language': 'en',
                    'country': 'in',
                    'category': 'business',
                    'size': 5,
                    'from_date': today_str  # Only today's news
                }
                
                response = requests.get(
                    'https://newsdata.io/api/1/news',
                    params=params,
                    headers={'X-ACCESS-KEY': self.newsdata_api_key},
                    timeout=15,
                    verify=True  # Enable SSL verification
                )
                
                if response.status_code == 200:
                    data = response.json()
                    api_articles = data.get('results', [])
                    
                    # Filter for today's articles only
                    today_api_articles = []
                    for article in api_articles:
                        pub_date = article.get('pubDate', '')
                        if self.is_today_news(pub_date):
                            article['source_type'] = 'api'
                            today_api_articles.append(article)
                    
                    all_articles.extend(today_api_articles)
                    data_sources.append(f"NewsData API ({len(today_api_articles)} today)")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: API found {len(today_api_articles)} articles from today")
                        
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: API fetch error: {e}")
        
        # 3. AI Deduplication (if available)
        unique_articles = all_articles
        dedup_stats = {'method': 'none', 'original_count': len(all_articles), 'deduplicated_count': len(all_articles)}
        
        if AI_DEDUPLICATION_AVAILABLE and len(all_articles) >= 3:
            try:
                dedup_result = ai_deduplicate_news_articles(all_articles)
                unique_articles = dedup_result.get('deduplicated_articles', all_articles)
                dedup_stats = dedup_result.get('stats', dedup_stats)
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: AI deduplication: {len(all_articles)} → {len(unique_articles)} articles")
                    
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: AI deduplication failed: {e}")
        
        return {
            'success': len(unique_articles) > 0,
            'articles': unique_articles,
            'total_articles': len(unique_articles),
            'data_sources': data_sources,
            'deduplication_stats': dedup_stats,
            'company_name': company_name,
            'date_filter': 'today_only',
            'fetch_timestamp': datetime.now().isoformat()
        }
    
    def fetch_recent_news(self, company_name: str) -> Dict:
        """Fetch and filter recent news (last 48 hours)"""
        all_articles = []
        data_sources = []
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Fetching recent news for {company_name}")
        
        # 1. Fetch from RSS feeds (real-time)
        if RSS_AVAILABLE:
            try:
                rss_fetcher = RSSNewsFetcher()
                rss_result = rss_fetcher.fetch_comprehensive_rss_news(company_name)
                
                if rss_result.get('success'):
                    rss_articles = rss_result.get('articles', [])
                    
                    # Filter for recent articles only
                    recent_articles = []
                    for article in rss_articles:
                        pub_date = article.get('pubDate', article.get('published_at', ''))
                        # Only include recent articles
                        if self.is_recent_news(pub_date):
                            article['source_type'] = 'rss'
                            recent_articles.append(article)
                    
                    all_articles.extend(recent_articles)
                    data_sources.append(f"RSS Feeds ({len(recent_articles)} recent)")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: RSS found {len(recent_articles)} recent articles (filtered from {len(rss_articles)} total)")
                        
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: RSS fetch error: {e}")
        
        # 2. Fetch from NewsData.io API (backup, also filter for recent)
        if self.newsdata_api_key and len(all_articles) < 10:  # Only if we need more articles
            try:
                # Use recent date for API search (last 2 days)
                from_date = (self.today - timedelta(days=2)).strftime('%Y-%m-%d')
                
                params = {
                    'q': f'"{company_name}"',
                    'language': 'en',
                    'country': 'in',
                    'category': 'business',
                    'size': 10,
                    'from_date': from_date  # Recent news
                }
                
                response = requests.get(
                    'https://newsdata.io/api/1/news',
                    params=params,
                    headers={'X-ACCESS-KEY': self.newsdata_api_key},
                    timeout=15,
                    verify=True  # Enable SSL verification
                )
                
                if response.status_code == 200:
                    data = response.json()
                    api_articles = data.get('results', [])
                    
                    # Filter for recent articles only
                    recent_api_articles = []
                    for article in api_articles:
                        pub_date = article.get('pubDate', '')
                        if self.is_recent_news(pub_date):
                            article['source_type'] = 'api'
                            recent_api_articles.append(article)
                    
                    all_articles.extend(recent_api_articles)
                    data_sources.append(f"NewsData API ({len(recent_api_articles)} recent)")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: API found {len(recent_api_articles)} recent articles")
                        
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: API fetch error: {e}")
        
        # 3. AI Deduplication (if available)
        unique_articles = all_articles
        dedup_stats = {'method': 'none', 'original_count': len(all_articles), 'deduplicated_count': len(all_articles)}
        
        if AI_DEDUPLICATION_AVAILABLE and len(all_articles) >= 3:
            try:
                dedup_result = ai_deduplicate_news_articles(all_articles)
                unique_articles = dedup_result.get('deduplicated_articles', all_articles)
                dedup_stats = dedup_result.get('stats', dedup_stats)
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: AI deduplication: {len(all_articles)} → {len(unique_articles)} articles")
                    
            except Exception as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: AI deduplication failed: {e}")
        
        return {
            'success': len(unique_articles) > 0,
            'articles': unique_articles,
            'total_articles': len(unique_articles),
            'data_sources': data_sources,
            'deduplication_stats': dedup_stats,
            'company_name': company_name,
            'date_filter': 'recent_48_hours',
            'fetch_timestamp': datetime.now().isoformat()
        }
    
    def _get_source_summary(self, articles: List[Dict]) -> str:
        """Get a summary of sources for the articles"""
        sources = set(article.get('source', 'Unknown') for article in articles)
        return ', '.join(sorted(sources)) if sources else 'Unknown sources'
    
    def format_crisp_telegram_message(self, company_name: str, articles: List[Dict], ai_summary: str, dedup_stats: Dict = None) -> str:
        """Format crisp Telegram message without URLs"""
        if not articles:
            return f"📰 No news for {company_name} today"
        
        # Header with today's date
        today_formatted = self.today.strftime('%B %d, %Y')
        
        # Deduplication info (brief)
        dedup_info = ""
        if dedup_stats and dedup_stats.get('original_count', 0) > dedup_stats.get('deduplicated_count', 0):
            original = dedup_stats.get('original_count', 0)
            final = dedup_stats.get('deduplicated_count', 0)
            dedup_info = f" (📊 {original}→{final})"
        
        # Build message
        message = f"""📰 TODAY'S NEWS: {company_name}
📅 {today_formatted}{dedup_info}

💡 {ai_summary}

📊 Coverage: {len(articles)} articles from {self._get_source_summary(articles)}

"""
        
        # Add brief article list (titles only, no URLs)
        if len(articles) <= 3:
            message += "📋 Headlines:\n"
            for i, article in enumerate(articles, 1):
                title = article.get('title', 'Untitled')
                source = article.get('source', 'Unknown')
                
                # Truncate long titles
                if len(title) > 50:
                    title = title[:50] + '...'
                
                message += f"{i}. {title} ({source})\n"
        else:
            # Just show count and sources for many articles
            message += f"📋 {len(articles)} articles from various sources"
        
        return message.strip()

def enhanced_send_news_alerts(user_client, user_id: str, monitored_scrips, telegram_recipients) -> int:
    """Send enhanced news alerts to Telegram recipients"""
    messages_sent = 0
    
    try:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Starting enhanced news alerts for user {user_id}")
        
        # Create news monitor instance
        news_monitor = EnhancedNewsMonitor()
        
        # Process each monitored scrip
        for scrip in monitored_scrips:
            company_name = scrip.get('company_name', '')
            bse_code = scrip.get('bse_code', '')
            
            if not company_name:
                continue
                
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Processing {company_name} ({bse_code})")
            
            # Fetch today's news for this company
            news_result = news_monitor.fetch_today_news_only(company_name)
            
            if not news_result.get('success'):
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No news for {company_name} today")
                continue
            
            articles = news_result.get('articles', [])
            if not articles:
                continue
            
            # Filter out articles that have already been sent
            new_articles = []
            for article in articles:
                if not check_news_already_sent(user_client, article, company_name):
                    new_articles.append(article)
                else:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        title = article.get('title', 'Unknown')[:50]
                        print(f"NEWS: Skipping already sent article: {title}")
            
            if not new_articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No new articles for {company_name}")
                continue
            
            # Generate AI summary for new articles only
            ai_summary = news_monitor.generate_ai_summary(new_articles, company_name)
            
            # Format Telegram message
            telegram_message = news_monitor.format_crisp_telegram_message(
                company_name, 
                new_articles, 
                ai_summary,
                news_result.get('deduplication_stats')
            )
            
            # Send to all recipients
            for recipient in telegram_recipients:
                chat_id = recipient['chat_id']
                user_name = recipient.get('user_name', 'User')
                
                # Add user name header
                personalized_message = f"👤 {user_name}\n" + "─" * 20 + "\n" + telegram_message
                
                try:
                    from database import send_telegram_message_with_user_name
                    if send_telegram_message_with_user_name(chat_id, personalized_message, user_name):
                        messages_sent += 1
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"NEWS: Sent alert to {user_name}")
                    else:
                        print(f"NEWS: Failed to send alert to {user_name}")
                except Exception as e:
                    print(f"NEWS: Error sending to {user_name}: {e}")
            
            # Store the sent articles to prevent duplicates in future
            for article in new_articles:
                store_sent_news_article(user_client, article, company_name, user_id)
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Enhanced alerts completed. Messages sent: {messages_sent}")
            
    except Exception as e:
        print(f"NEWS: Error in enhanced_send_news_alerts: {e}")
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            import traceback
            traceback.print_exc()
    
    return messages_sent