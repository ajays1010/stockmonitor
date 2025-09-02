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

class EnhancedNewsMonitor:
    """Enhanced news monitoring with user feedback improvements"""
    
    def __init__(self):
        self.today = datetime.now().date()
        self.ai_api_key = os.environ.get('GOOGLE_API_KEY')
        self.newsdata_api_key = os.environ.get('NEWSDATA_API_KEY')
        
    def is_today_news(self, pub_date_str: str) -> bool:
        """Check if article is from today"""
        if not pub_date_str:
            return False
            
        try:
            # Parse various date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%fZ'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(pub_date_str.strip(), fmt)
                    return dt.date() == self.today
                except ValueError:
                    continue
                    
            return False
        except Exception:
            return False
    
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
            
            # Call Gemini API
            response = requests.post(
                f'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key={self.ai_api_key}',
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{
                        'parts': [{'text': prompt}]
                    }]
                },
                timeout=15
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
        if self.newsdata_api_key and len(all_articles) < 3:  # Only if we need more articles
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
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    api_articles = data.get('results', [])
                    
                    # Double-check date filtering
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
            sources = list(set([article.get('source', 'Unknown') for article in articles]))
            message += f"📋 Sources: {', '.join(sources[:3])}"
            if len(sources) > 3:
                message += f" +{len(sources)-3} more"
            message += "\n"
        
        # Footer
        message += "\n💡 Use Sentiment Analysis in app for detailed market impact!"
        
        return message
    
    def _get_source_summary(self, articles: List[Dict]) -> str:
        """Get brief summary of news sources"""
        sources = [article.get('source', 'Unknown') for article in articles]
        unique_sources = list(set(sources))
        
        if len(unique_sources) == 1:
            return unique_sources[0]
        elif len(unique_sources) <= 3:
            return ', '.join(unique_sources)
        else:
            return f"{unique_sources[0]}, {unique_sources[1]} +{len(unique_sources)-2} more"

def enhanced_send_news_alerts(user_client, user_id: str, monitored_scrips: List[Dict], telegram_recipients: List[Dict]) -> int:
    """
    Enhanced news alerts function - replacement for existing news monitoring
    Focuses on today's news with crisp summaries
    """
    messages_sent = 0
    monitor = EnhancedNewsMonitor()
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Starting enhanced today-only news monitoring for user {user_id}")
    
    for scrip in monitored_scrips:
        company_name = scrip.get('company_name', '')
        bse_code = scrip.get('bse_code', '')
        
        if not company_name:
            continue
        
        try:
            # Fetch today's news only
            news_result = monitor.fetch_today_news_only(company_name)
            
            if not news_result.get('success') or not news_result.get('articles'):
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No news today for {company_name}")
                continue
            
            articles = news_result.get('articles', [])
            dedup_stats = news_result.get('deduplication_stats', {})
            
            # Filter out already sent articles (check against database)
            new_articles = []
            for article in articles:
                if not check_news_already_sent_today(user_client, article, company_name):
                    new_articles.append(article)
            
            if not new_articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No new articles today for {company_name}")
                continue
            
            # Generate AI summary
            ai_summary = monitor.generate_ai_summary(new_articles, company_name)
            
            # Format crisp message
            message = monitor.format_crisp_telegram_message(company_name, new_articles, ai_summary, dedup_stats)
            
            # Send to Telegram recipients
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
                            'disable_web_page_preview': True  # No URL previews for clean look
                        },
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        messages_sent += 1
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"NEWS: Sent enhanced crisp alert for {company_name} to {chat_id}")
                    else:
                        # Parse Telegram API error
                        try:
                            error_data = response.json()
                            error_code = error_data.get('error_code', 'unknown')
                            error_desc = error_data.get('description', 'unknown')
                            
                            if error_code == 400 and 'chat not found' in error_desc.lower():
                                print(f"NEWS: Chat {chat_id} not found - user may have blocked bot or deleted chat")
                                # TODO: Mark this recipient as inactive in database
                            elif error_code == 403 and 'bot was blocked' in error_desc.lower():
                                print(f"NEWS: Bot was blocked by user {chat_id}")
                                # TODO: Mark this recipient as inactive in database  
                            else:
                                print(f"NEWS: Telegram API error for {chat_id}: {error_desc} (code: {error_code})")
                        except:
                            print(f"NEWS: Telegram API error for {chat_id}: {response.text}")
                        
                except Exception as send_error:
                    print(f"NEWS: Error sending to {chat_id}: {send_error}")
            
            # Store articles for future reference
            for article in new_articles:
                store_news_article_enhanced(user_client, article, company_name, [user_id])
            
        except Exception as e:
            print(f"NEWS: Error processing {company_name}: {e}")
            continue
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Enhanced monitoring complete for user {user_id}, sent {messages_sent} crisp alerts")
    
    return messages_sent

def check_news_already_sent_today(user_client, article: Dict, company_name: str) -> bool:
    """Check if this article was already sent today"""
    try:
        article_id = article.get('article_id', article.get('link', ''))
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        response = user_client.table('processed_news_articles').select('article_id').eq('article_id', article_id).gte('created_at', today_start.isoformat()).execute()
        
        return len(response.data) > 0
    except Exception:
        return False

def store_news_article_enhanced(user_client, article: Dict, company_name: str, user_ids: List[str]):
    """Store article with enhanced tracking - BULLETPROOF database-safe version"""
    try:
        # DEFENSIVE APPROACH: Extract fields safely and create completely clean data structure
        # This prevents ANY problematic fields from reaching the database
        
        # Extract article ID safely
        article_id = ''
        if article.get('article_id'):
            article_id = str(article.get('article_id', ''))
        elif article.get('link'):
            article_id = str(article.get('link', ''))
        elif article.get('url'):
            article_id = str(article.get('url', ''))
        else:
            # Generate a unique ID if none available
            import hashlib
            import time
            article_id = hashlib.md5(f"{company_name}_{time.time()}".encode()).hexdigest()[:16]
        
        # Extract other fields safely
        title = str(article.get('title', '') or '').strip()
        description = str(article.get('description', '') or '').strip()
        
        # Get URL - try multiple possible field names
        url = ''
        if article.get('url'):
            url = str(article.get('url', ''))
        elif article.get('link'):
            url = str(article.get('link', ''))
        
        # Get source name safely
        source_name = str(article.get('source', article.get('source_name', 'Unknown')) or 'Unknown').strip()
        
        # Get publication date safely
        pub_date = str(article.get('pubDate', article.get('published_at', '')) or '').strip()
        
        # Get source type safely (optional)
        source_type = str(article.get('source_type', '') or '').strip()
        
        # Create ONLY the fields that exist in database - HARDCODED list for safety
        safe_article_data = {
            'article_id': article_id,
            'title': title,
            'description': description,
            'url': url,
            'source_name': source_name,
            'pub_date': pub_date,
            'stock_query': str(company_name or '').strip(),
            'sent_to_users': user_ids or []
        }
        
        # Add source_type only if it has a value
        if source_type:
            safe_article_data['source_type'] = source_type
        
        # DATABASE INSERT - using only safe fields
        user_client.table('processed_news_articles').insert(safe_article_data).execute()
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Stored article {article_id[:12]}... for {company_name}")
            
    except Exception as e:
        # Enhanced error logging with field information
        error_msg = str(e)
        if 'column' in error_msg and 'schema cache' in error_msg:
            print(f"NEWS: Database schema error - article storage skipped: {error_msg}")
            print(f"NEWS: This should NOT happen anymore - please report this error")
        elif 'PGRST204' in error_msg:
            print(f"NEWS: PostgREST schema error - article storage skipped: {error_msg}")
            print(f"NEWS: This indicates a remaining schema mismatch")
        else:
            print(f"NEWS: Article storage error: {e}")

# For compatibility with existing system
def send_news_alerts_enhanced(user_client, user_id: str, monitored_scrips: List[Dict], telegram_recipients: List[Dict]) -> int:
    """Wrapper function for enhanced news alerts"""
    return enhanced_send_news_alerts(user_client, user_id, monitored_scrips, telegram_recipients)