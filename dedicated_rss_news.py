#!/usr/bin/env python3
"""
DEDICATED RSS NEWS PROCESSOR
Single script to handle all RSS news functionality with proper duplicate prevention.

Features:
- Uses existing news_sent_tracking table
- Comprehensive blacklisting and filtering
- Multiple search strategies
- Memory-efficient processing
- Smart duplicate detection
"""

import requests
import feedparser
import hashlib
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote_plus
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DedicatedRSSProcessor:
    """Dedicated RSS news processor with comprehensive duplicate prevention"""
    
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'}
        self.timeout = 8
        
        # Blacklist for noise filtering
        self.blacklist_words = [
            'stocks to watch', 'stock picks', 'top gainers', 'top losers', 
            'market movers', 'recommendations', 'buy sell', 'target price',
            'technical analysis', 'chart analysis', 'support resistance',
            'breakout', 'stock tips', 'trading tips', 'intraday',
            'multibagger', 'penny stock', 'smallcap', 'midcap',
            'why is', 'why did', 'should you buy', 'should you sell'
        ]
        
        self.blacklisted_sources = ['stockgro', 'trade brains', 'stock axis']
    
    def fetch_news_for_company(self, company_name: str) -> List[Dict]:
        """Fetch top 10 articles for a single company from quality sources"""
        
        # Multiple search queries to catch different types of news
        search_queries = [
            f'"{company_name}" India stock',
            f'"{company_name}" order',
            f'"{company_name}" news',
            f'"{company_name}" results',
            f'"{company_name}" announcement',
            f'"{company_name}" earnings',
            f'"{company_name}" contract'
        ]
        
        all_articles = []
        
        for search_query in search_queries:
            try:
                articles = self._fetch_google_news(search_query)
                for article in articles:
                    article['company'] = company_name
                    article['search_query'] = search_query
                all_articles.extend(articles)
                
            except Exception as e:
                logger.warning(f"Search query '{search_query}' failed: {e}")
                continue
        
        # Filter and deduplicate
        filtered_articles = self._filter_and_deduplicate(all_articles, company_name)
        
        # Filter for quality sources only
        quality_articles = self._filter_quality_sources(filtered_articles)
        
        return quality_articles[:10]  # Top 10 articles per company
    
    def _fetch_google_news(self, search_query: str) -> List[Dict]:
        """Fetch news from Google News RSS"""
        try:
            search_encoded = quote_plus(search_query)
            url = f'https://news.google.com/rss/search?q={search_encoded}&hl=en&gl=IN&ceid=IN:en'
            
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            if response.status_code != 200:
                return []
            
            feed = feedparser.parse(response.content)
            articles = []
            
            # Process first 8 entries from each query to get more articles
            for entry in feed.entries[:8]:
                title = entry.get('title', '').strip()
                link = entry.get('link', '').strip()
                pub_date = entry.get('published', '')
                
                if not title or len(title) < 15:
                    continue
                
                # Extract source from Google News title format
                source = 'Google News'
                if ' - ' in title:
                    parts = title.split(' - ')
                    if len(parts) >= 2:
                        source = parts[-1].strip()
                        title = ' - '.join(parts[:-1]).strip()
                
                articles.append({
                    'title': title[:150],  # Truncate long titles
                    'source': source,
                    'link': link,
                    'pubDate': pub_date,
                    'source_type': 'google_news_rss'
                })
            
            return articles
            
        except Exception as e:
            logger.error(f"Google News fetch error: {e}")
            return []
    
    def _filter_and_deduplicate(self, articles: List[Dict], company_name: str) -> List[Dict]:
        """Filter articles for relevance and remove duplicates"""
        filtered_articles = []
        
        for article in articles:
            # Relevance check
            if not self._is_relevant(article, company_name):
                continue
            
            # Blacklist check
            if self._is_blacklisted(article):
                continue
            
            # Duplicate check within current batch
            if self._is_duplicate_in_batch(article, filtered_articles):
                continue
            
            filtered_articles.append(article)
        
        return filtered_articles
    
    def _filter_quality_sources(self, articles: List[Dict]) -> List[Dict]:
        """Filter articles to keep only quality news sources"""
        quality_sources = [
            'economic times', 'et now', 'economictimes',
            'moneycontrol', 'money control',
            'livemint', 'live mint', 'mint',
            'business standard', 'business today',
            'financial express', 'cnbc tv18', 'cnbctv18',
            'reuters', 'bloomberg', 'ndtv profit',
            'hindu businessline', 'businessline',
            'zeebiz', 'zee business'
        ]
        
        quality_articles = []
        for article in articles:
            source = article.get('source', '').lower()
            
            # Check if source matches any quality source
            is_quality = any(quality_src in source for quality_src in quality_sources)
            
            if is_quality:
                quality_articles.append(article)
        
        # If no quality sources found, return original articles (better than nothing)
        return quality_articles if quality_articles else articles[:3]
    
    def _is_relevant(self, article: Dict, company_name: str) -> bool:
        """Check if article is relevant to the company"""
        title = article.get('title', '').lower()
        company_lower = company_name.lower()
        
        # Must mention company name (first 2 words)
        company_words = company_lower.split()[:2]
        if not any(word in title for word in company_words):
            return False
        
        return True
    
    def _is_blacklisted(self, article: Dict) -> bool:
        """Check if article should be blacklisted"""
        title = article.get('title', '').lower()
        source = article.get('source', '').lower()
        
        # Check blacklisted sources
        if any(blocked_source in source for blocked_source in self.blacklisted_sources):
            return True
        
        # Check blacklisted phrases
        if any(phrase in title for phrase in self.blacklist_words):
            return True
        
        return False
    
    def _is_duplicate_in_batch(self, article: Dict, existing_articles: List[Dict]) -> bool:
        """Check if article is duplicate within current batch"""
        title = article.get('title', '')[:60].lower()
        
        for existing in existing_articles:
            existing_title = existing.get('title', '')[:60].lower()
            
            # Same title check
            if title == existing_title:
                return True
            
            # Similarity check
            if self._calculate_similarity(title, existing_title) > 0.7:
                return True
        
        return False
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts"""
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if len(words1) == 0 or len(words2) == 0:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def generate_article_hash(self, article: Dict, user_id: str, recipient_id: str) -> str:
        """Generate unique hash for duplicate tracking"""
        title = article.get('title', '')[:100]
        company = article.get('company', '')
        source = article.get('source', '')
        
        hash_string = f"{title}|{company}|{source}|{user_id}|{recipient_id}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def is_already_sent(self, sb, article: Dict, user_id: str, recipient_id: str) -> bool:
        """Check if article was already sent using news_sent_tracking table"""
        try:
            # Generate article identifier
            title = article.get('title', '')[:100]
            company = article.get('company', '')
            
            # Check in news_sent_tracking table
            result = sb.table('news_sent_tracking').select('id').eq(
                'user_id', user_id
            ).eq(
                'company_name', company
            ).ilike(
                'news_title', f"%{title[:50]}%"  # Partial match for title
            ).limit(1).execute()
            
            return len(result.data) > 0
            
        except Exception as e:
            logger.warning(f"Error checking duplicate: {e}")
            return False
    
    def mark_as_sent(self, sb, article: Dict, user_id: str, recipient_id: str):
        """Mark article as sent in news_sent_tracking table"""
        try:
            tracking_data = {
                'user_id': user_id,
                'recipient_id': recipient_id,
                'company_name': article.get('company', ''),
                'news_title': article.get('title', '')[:200],
                'news_source': article.get('source', ''),
                'news_url': article.get('link', ''),
                'sent_at': datetime.now().isoformat(),
                'source': 'rss_dedicated',
                'news_type': 'rss_news'
            }
            
            sb.table('news_sent_tracking').insert(tracking_data).execute()
            logger.debug(f"Marked as sent: {article.get('title', '')[:50]}...")
            
        except Exception as e:
            logger.warning(f"Failed to mark as sent: {e}")
    
    def format_message(self, company_name: str, articles: List[Dict]) -> str:
        """Format articles into clean Telegram message"""
        if not articles:
            return ""
        
        message_parts = [f"📰 {company_name} News:"]
        
        for i, article in enumerate(articles, 1):
            title = article.get('title', 'No title')
            source = article.get('source', 'Unknown')
            message_parts.append(f"{i}. {title} ({source})")
        
        return "\n".join(message_parts)
    
    def format_multi_company_message(self, articles_by_company: Dict[str, List[Dict]]) -> str:
        """Format articles from multiple companies into one message"""
        if not articles_by_company:
            return ""
        
        message_parts = ["📰 Latest News Updates:"]
        
        for company, articles in articles_by_company.items():
            message_parts.append(f"\n🏢 {company}:")
            for i, article in enumerate(articles, 1):
                title = article.get('title', 'No title')
                source = article.get('source', 'Unknown')
                message_parts.append(f"  {i}. {title} ({source})")
        
        return "\n".join(message_parts)

def process_rss_news_for_user(sb, user_id: str, scrips: List[Dict], recipients: List[Dict]) -> int:
    """
    Main function to process RSS news for a user
    Processes ALL companies and fetches top 10 news after blacklist filtering
    """
    messages_sent = 0
    
    try:
        processor = DedicatedRSSProcessor()
        
        if not scrips:
            return 0
        
        print(f"📰 DEDICATED RSS: Processing {len(scrips)} companies for user {user_id[:8]}...")
        
        # Collect articles from ALL companies
        all_company_articles = []
        
        for i, scrip in enumerate(scrips, 1):
            company_name = scrip.get('company_name', '')
            if not company_name:
                continue
            
            print(f"📰 Processing {i}/{len(scrips)}: {company_name}")
            
            try:
                # Fetch news for this company
                company_articles = processor.fetch_news_for_company(company_name)
                
                if company_articles:
                    print(f"📰 Found {len(company_articles)} articles for {company_name}")
                    all_company_articles.extend(company_articles)
                else:
                    print(f"📰 No articles found for {company_name}")
                    
            except Exception as e:
                print(f"📰 ❌ Error processing {company_name}: {e}")
                continue
        
        if not all_company_articles:
            print(f"📰 No articles found for any company")
            return 0
        
        print(f"📰 Total articles collected: {len(all_company_articles)}")
        
        # Group articles by company (each company can have up to 10 articles)
        articles_by_company = {}
        for article in all_company_articles:
            company = article.get('company', 'Unknown')
            if company not in articles_by_company:
                articles_by_company[company] = []
            articles_by_company[company].append(article)
        
        # Show summary
        for company, articles in articles_by_company.items():
            print(f"📰 {company}: {len(articles)} articles")
        
        # Send to each recipient
        for recipient in recipients:
            try:
                chat_id = recipient['chat_id']
                user_name = recipient.get('user_name', 'User')
                
                # Filter out already sent articles for this recipient
                new_articles_by_company = {}
                total_new_articles = 0
                
                for company, company_articles in articles_by_company.items():
                    new_company_articles = []
                    for article in company_articles:
                        if not processor.is_already_sent(sb, article, user_id, chat_id):
                            new_company_articles.append(article)
                            total_new_articles += 1
                    
                    if new_company_articles:
                        new_articles_by_company[company] = new_company_articles
                
                if not new_articles_by_company:
                    print(f"📰 No new articles for {user_name}")
                    continue
                
                print(f"📰 Sending {total_new_articles} new articles to {user_name}")
                
                # Format message with all companies
                message = processor.format_multi_company_message(new_articles_by_company)
                
                # Send via Telegram
                from database import send_telegram_message_with_user_name
                if send_telegram_message_with_user_name(chat_id, message, user_name):
                    messages_sent += 1
                    print(f"📰 ✅ Sent to {user_name}")
                    
                    # Mark all articles as sent
                    for company_articles in new_articles_by_company.values():
                        for article in company_articles:
                            processor.mark_as_sent(sb, article, user_id, chat_id)
                else:
                    print(f"📰 ❌ Failed to send to {user_name}")
                
            except Exception as e:
                print(f"📰 ❌ Error processing recipient {recipient.get('user_name', 'unknown')}: {e}")
                continue
    
    except Exception as e:
        print(f"❌ DEDICATED RSS ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        import gc
        gc.collect()
    
    return messages_sent

if __name__ == "__main__":
    # Test the dedicated RSS processor
    processor = DedicatedRSSProcessor()
    articles = processor.fetch_news_for_company("Adani Power Ltd")
    
    print(f"🧪 Test: Found {len(articles)} articles for Adani Power")
    for i, article in enumerate(articles, 1):
        print(f"  {i}. {article['title'][:60]}... ({article['source']})")