#!/usr/bin/env python3
"""
RSS News Fetcher for BSE Monitor
Fetches news from RSS feeds as a complement to NewsData.io API

Features:
1. Google News RSS for company-specific news
2. Major Indian financial news RSS feeds  
3. Real-time news updates (no 12-hour delay)
4. Free alternative to paid news APIs
"""

import feedparser
import requests
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
import time
from urllib.parse import quote_plus, urlencode
import xml.etree.ElementTree as ET

class RSSNewsFetcher:
    """RSS-based news fetcher for real-time news"""
    
    def __init__(self):
        # Major Indian financial news RSS feeds (removed problematic Business Standard)
        self.indian_finance_feeds = {
            'economic_times': 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms',
            # 'business_standard': 'https://www.business-standard.com/rss/markets-106.rss',  # Removed due to 403 errors
            'moneycontrol': 'https://www.moneycontrol.com/rss/business.xml',
            'financial_express': 'https://www.financialexpress.com/market/rss/',
            'livemint': 'https://www.livemint.com/rss/markets',
            'zeebiz': 'https://www.zeebiz.com/market/rss',
            'cnbctv18': 'https://www.cnbctv18.com/market/rss.xml'  # Added more reliable source
        }
        
        # Updated headers to avoid blocking
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Rate limiting
        self.last_request_time = {}
        self.min_delay = 3.0  # Increased delay to 3 seconds between requests to same domain
    
    def _make_request_with_retry(self, url: str, max_retries: int = 3) -> requests.Response:
        """
        Make HTTP request with retry logic and proper SSL verification
        """
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # Always use SSL verification - this fixes the InsecureRequestWarning
                response = requests.get(
                    url,
                    headers=self.headers,
                    timeout=20,  # Increased timeout
                    verify=True,  # Enable SSL verification
                    allow_redirects=True,
                    stream=False
                )
                
                # Check for specific status codes
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    # If we get a 403, try with a different User-Agent
                    alt_headers = self.headers.copy()
                    alt_headers['User-Agent'] = 'FeedFetcher-Google; (+http://www.google.com/feedfetcher.html)'
                    
                    alt_response = requests.get(
                        url,
                        headers=alt_headers,
                        timeout=20,
                        verify=True,
                        allow_redirects=True,
                        stream=False
                    )
                    
                    if alt_response.status_code == 200:
                        return alt_response
                    else:
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"HTTP {response.status_code} for {url} (attempt {attempt + 1}) - Also failed with alt User-Agent: {alt_response.status_code}")
                else:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"HTTP {response.status_code} for {url} (attempt {attempt + 1})")
                    
            except requests.exceptions.SSLError as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"SSL error for {url}: {str(e)[:100]}")
                last_exception = e
                # Don't retry SSL errors as they're usually configuration issues
                break
            except requests.exceptions.RequestException as e:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"Request error for {url} (attempt {attempt + 1}): {str(e)[:100]}")
                last_exception = e
                continue
            
            # Wait before retry - increased delay
            if attempt < max_retries - 1:
                time.sleep(3 ** attempt)  # Exponential backoff with larger delays
        
        # If we get here, all attempts failed
        raise last_exception if last_exception else Exception(f"Failed to fetch {url} after {max_retries} attempts")
    
    def _rate_limit(self, feed_url: str):
        """Implement rate limiting per domain"""
        domain = feed_url.split('/')[2] if '//' in feed_url else feed_url
        
        if domain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[domain]
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)
        
        self.last_request_time[domain] = time.time()
    
    def fetch_google_news_rss(self, company_name: str, language: str = 'en', country: str = 'IN') -> Dict:
        """
        Fetch company-specific news from Google News RSS
        Enhanced with better connectivity and retry mechanisms
        """
        try:
            # Clean and encode company name for Google News search
            search_query = quote_plus(f'"{company_name}" stock market India')
            
            # Google News RSS URL
            google_news_url = f'https://news.google.com/rss/search?q={search_query}&hl={language}&gl={country}&ceid={country}:{language}'
            
            self._rate_limit(google_news_url)
            
            # Fetch RSS feed with retry mechanism
            response = self._make_request_with_retry(google_news_url)
            
            # Parse RSS feed - use raw content to avoid encoding issues
            feed = feedparser.parse(response.content)
            
            articles = []
            for entry in feed.entries[:15]:  # Limit to top 15 articles
                # Clean title and description
                title = self._clean_text(entry.get('title', ''))
                description = self._clean_text(entry.get('description', entry.get('summary', '')))
                
                # Extract publication date
                pub_date = self._parse_date(entry.get('published', ''))
                
                # Extract source from title (Google News format: "Title - Source")
                source_name = self._extract_source_from_title(title)
                
                article = {
                    'article_id': entry.get('id', entry.get('link', '')),
                    'title': title,
                    'description': description,
                    'link': entry.get('link', ''),
                    'url': entry.get('link', ''),
                    'source_name': source_name,
                    'source': source_name,
                    'pubDate': pub_date,
                    'published_at': pub_date,
                    'source_type': 'google_news_rss',
                    'language': language,
                    'country': country
                }
                articles.append(article)
            
            return {
                'success': True,
                'articles': articles,
                'total_results': len(articles),
                'source': 'google_news_rss',
                'search_query': company_name,
                'feed_url': google_news_url
            }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'Google News RSS error: {str(e)[:100]}',
                'source': 'google_news_rss'
            }
    
    def fetch_financial_news_feeds(self, company_keywords: List[str]) -> Dict:
        """
        Fetch news from major Indian financial RSS feeds
        Enhanced with better connectivity and individual feed retry
        """
        all_articles = []
        feed_results = {}
        
        for feed_name, feed_url in self.indian_finance_feeds.items():
            try:
                self._rate_limit(feed_url)
                
                # Special handling for Business Standard which often returns 403
                if feed_name == 'business_standard':
                    # Try with alternative approach for Business Standard
                    articles = self._fetch_business_standard_feed(company_keywords)
                    if articles:
                        feed_results[feed_name] = {
                            'success': True,
                            'articles': articles,
                            'count': len(articles)
                        }
                        all_articles.extend(articles)
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"✅ {feed_name}: {len(articles)} articles (alternative method)")
                        continue
                
                # Fetch RSS feed with retry mechanism
                response = self._make_request_with_retry(feed_url)
                
                # Parse RSS feed - use raw content to avoid encoding issues
                feed = feedparser.parse(response.content)
                
                feed_articles = []
                for entry in feed.entries[:20]:  # Limit per feed
                    title = self._clean_text(entry.get('title', ''))
                    description = self._clean_text(entry.get('description', entry.get('summary', '')))
                    
                    # Check if article mentions any of the company keywords
                    if self._contains_company_keywords(title + ' ' + description, company_keywords):
                        pub_date = self._parse_date(entry.get('published', ''))
                        
                        article = {
                            'article_id': entry.get('id', entry.get('link', '')),
                            'title': title,
                            'description': description,
                            'link': entry.get('link', ''),
                            'url': entry.get('link', ''),
                            'source_name': feed_name.replace('_', ' ').title(),
                            'source': feed_name.replace('_', ' ').title(),
                            'pubDate': pub_date,
                            'published_at': pub_date,
                            'source_type': 'financial_rss',
                            'feed_name': feed_name
                        }
                        feed_articles.append(article)
                
                feed_results[feed_name] = {
                    'success': True,
                    'articles': feed_articles,
                    'count': len(feed_articles)
                }
                all_articles.extend(feed_articles)
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"✅ {feed_name}: {len(feed_articles)} articles")
                    
            except Exception as e:
                # Individual feed failure - continue with others
                feed_results[feed_name] = {
                    'success': False,
                    'error': f'Error: {str(e)[:50]}',
                    'count': 0
                }
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"❌ {feed_name}: {str(e)[:100]}")
        
        return {
            'success': True,  # Always return success even if no articles found
            'articles': all_articles,
            'total_results': len(all_articles),
            'source': 'financial_rss_feeds',
            'feed_details': feed_results
        }
    
    def _fetch_business_standard_feed(self, company_keywords: List[str]) -> List[Dict]:
        """
        Alternative method to fetch Business Standard feed
        """
        try:
            # Try with a different approach for Business Standard
            alt_headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(
                'https://www.business-standard.com/rss/markets-106.rss',
                headers=alt_headers,
                timeout=20,
                verify=True
            )
            
            if response.status_code == 200:
                feed = feedparser.parse(response.content)
                articles = []
                
                for entry in feed.entries[:15]:
                    title = self._clean_text(entry.get('title', ''))
                    description = self._clean_text(entry.get('description', entry.get('summary', '')))
                    
                    # Check if article mentions any of the company keywords
                    if self._contains_company_keywords(title + ' ' + description, company_keywords):
                        pub_date = self._parse_date(entry.get('published', ''))
                        
                        article = {
                            'article_id': entry.get('id', entry.get('link', '')),
                            'title': title,
                            'description': description,
                            'link': entry.get('link', ''),
                            'url': entry.get('link', ''),
                            'source_name': 'Business Standard',
                            'source': 'Business Standard',
                            'pubDate': pub_date,
                            'published_at': pub_date,
                            'source_type': 'financial_rss',
                            'feed_name': 'business_standard'
                        }
                        articles.append(article)
                
                return articles
            else:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"Business Standard alt method failed with status {response.status_code}")
                return []
                
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"Business Standard alt method error: {e}")
            return []
    
    def fetch_comprehensive_rss_news(self, company_name: str) -> Dict:
        """
        Comprehensive RSS news fetching combining multiple sources
        Enhanced with better error handling and fallback mechanisms
        """
        all_articles = []
        data_sources = []
        
        # Generate company keywords
        company_keywords = self._generate_company_keywords(company_name)
        
        # 1. Try Google News RSS first (usually most reliable)
        try:
            google_result = self.fetch_google_news_rss(company_name)
            if google_result.get('success'):
                google_articles = google_result.get('articles', [])
                all_articles.extend(google_articles)
                data_sources.append(f"Google News RSS ({len(google_articles)} articles)")
            else:
                error_msg = google_result.get('error', 'unknown')
                data_sources.append(f"Google News RSS (failed: {error_msg[:50]}...)")
        except Exception as e:
            data_sources.append(f"Google News RSS (exception: {str(e)[:50]}...)")
        
        # 2. Try Financial RSS Feeds (but don't let failures block the whole process)
        try:
            financial_result = self.fetch_financial_news_feeds(company_keywords)
            if financial_result.get('success'):
                financial_articles = financial_result.get('articles', [])
                all_articles.extend(financial_articles)
                data_sources.append(f"Financial RSS Feeds ({len(financial_articles)} articles)")
                
                # Add feed-specific details for successful feeds only
                for feed_name, details in financial_result.get('feed_details', {}).items():
                    if details.get('success') and details.get('count', 0) > 0:
                        data_sources.append(f"  • {feed_name}: {details.get('count', 0)} articles")
            else:
                data_sources.append("Financial RSS Feeds (no matches found)")
        except Exception as e:
            data_sources.append(f"Financial RSS Feeds (exception: {str(e)[:50]}...)")
        
        # 3. If we have some articles, remove basic duplicates
        if all_articles:
            unique_articles = self._remove_duplicates(all_articles)
        else:
            unique_articles = []
        
        # 4. Return results with detailed status information
        return {
            'success': len(unique_articles) > 0,
            'articles': unique_articles,
            'total_articles': len(unique_articles),
            'duplicate_removed': len(all_articles) - len(unique_articles) if all_articles else 0,
            'data_sources': data_sources,
            'company_keywords_used': company_keywords,
            'source': 'comprehensive_rss',
            'rss_status': 'partial' if len(unique_articles) > 0 else 'failed',
            'debug_info': f"Tried {len(self.indian_finance_feeds) + 1} RSS sources"
        }
    
    def _generate_company_keywords(self, company_name: str) -> List[str]:
        """Generate search keywords from company name"""
        keywords = []
        
        # Add full name
        keywords.append(company_name.lower())
        
        # Clean name (remove Ltd, Limited, etc.)
        clean_name = company_name.replace(' Ltd', '').replace(' Limited', '').replace(' Pvt', '')
        if clean_name != company_name:
            keywords.append(clean_name.lower())
        
        # Extract brand name (first word)
        words = clean_name.split()
        if len(words) > 1:
            keywords.append(words[0].lower())
        
        # Special cases for major companies
        name_lower = company_name.lower()
        if 'ola electric' in name_lower:
            keywords.extend(['ola electric', 'ola ev', 'ola mobility'])
        elif 'reliance' in name_lower:
            keywords.extend(['reliance', 'ril'])
        elif 'tcs' in name_lower:
            keywords.extend(['tcs', 'tata consultancy'])
        elif 'infosys' in name_lower:
            keywords.extend(['infosys', 'infy'])
        
        return list(set(keywords))  # Remove duplicates
    
    def _contains_company_keywords(self, text: str, keywords: List[str]) -> bool:
        """Check if text contains any of the company keywords"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in keywords)
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ''
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove common RSS artifacts
        text = text.replace('&nbsp;', ' ').replace('&amp;', '&')
        
        return text.strip()
    
    def _extract_source_from_title(self, title: str) -> str:
        """Extract source name from Google News title format"""
        if ' - ' in title:
            parts = title.split(' - ')
            if len(parts) >= 2:
                return parts[-1].strip()  # Last part is usually the source
        return 'Google News'
    
    def _parse_date(self, date_str: str) -> str:
        """Parse various date formats to ISO format"""
        if not date_str:
            return datetime.now().isoformat()
        
        try:
            # Try parsing common RSS date formats
            formats = [
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S GMT',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If all parsing fails, return current time
            return datetime.now().isoformat()
            
        except Exception:
            return datetime.now().isoformat()
    
    def _remove_duplicates(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()
        
        for article in articles:
            title = article.get('title', '').lower().strip()
            title_key = title[:50] if title else str(len(unique_articles))
            
            if title_key not in seen_titles and title:
                seen_titles.add(title_key)
                unique_articles.append(article)
        
        return unique_articles

# Utility functions for integration
def fetch_rss_news_for_sentiment(company_name: str) -> Dict:
    """
    Main function to fetch RSS news for sentiment analysis
    Compatible with existing sentiment analysis system
    """
    fetcher = RSSNewsFetcher()
    return fetcher.fetch_comprehensive_rss_news(company_name)

if __name__ == "__main__":
    # Test the RSS fetcher
    fetcher = RSSNewsFetcher()
    
    print("🧪 Testing RSS News Fetcher...")
    
    # Test with OLA Electric
    result = fetcher.fetch_comprehensive_rss_news("OLA Electric Mobility Ltd")
    
    if result.get('success'):
        print(f"✅ Found {result.get('total_articles')} articles")
        print(f"📊 Sources: {', '.join(result.get('data_sources', []))}")
        
        articles = result.get('articles', [])
        if articles:
            print("\n📰 Sample Articles:")
            for i, article in enumerate(articles[:3], 1):
                title = article.get('title', 'No title')[:60]
                source = article.get('source', 'Unknown')
                print(f"  {i}. {title}... (Source: {source})")
    else:
        print("❌ No articles found")