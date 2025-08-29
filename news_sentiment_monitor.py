#!/usr/bin/env python3
"""
News Sentiment Monitoring Integration for BSE Monitor
Integrates with existing unified cron system and database patterns

This module provides:
1. News fetching for user's monitored stocks
2. Sentiment analysis with deduplication
3. Telegram notifications with news links and summaries
4. Database tracking for duplicate prevention
"""

import os
import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import time

# Sentiment Analysis Libraries (already in project)
from textblob import TextBlob
import nltk
from collections import Counter

# Ensure required NLTK data is available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

class NewsDataAPIClient:
    """NewsData.io API client optimized for stock news with rate limiting"""
    
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

class StockSentimentAnalyzer:
    """Sentiment analysis optimized for financial news"""
    
    def __init__(self):
        # Financial sentiment keywords
        self.positive_keywords = {
            'surge', 'rally', 'gain', 'profit', 'growth', 'bullish', 'upbeat', 'optimistic',
            'expansion', 'strong', 'robust', 'excellent', 'outstanding', 'record', 'milestone',
            'breakthrough', 'success', 'achievement', 'boost', 'rise', 'increase', 'jump',
            'soar', 'climb', 'advance', 'improvement', 'upgrade', 'buy', 'recommend', 'outperform'
        }
        
        self.negative_keywords = {
            'fall', 'drop', 'decline', 'loss', 'bearish', 'pessimistic', 'concern', 'worry',
            'downgrade', 'sell', 'avoid', 'risk', 'crash', 'plunge', 'tumble', 'slide',
            'weak', 'poor', 'disappointing', 'trouble', 'crisis', 'problem', 'issue',
            'challenge', 'threat', 'warning', 'alert', 'caution', 'volatile', 'uncertainty',
            'underperform', 'negative', 'losses'
        }
        
        # Weight multipliers
        self.title_weight = 2.0
        self.description_weight = 1.5
        self.keyword_weight = 1.2
    
    def analyze_article_sentiment(self, article: Dict) -> Dict:
        """Comprehensive sentiment analysis for a single article"""
        title = article.get('title', '') or ''
        description = article.get('description', '') or ''
        keywords = article.get('keywords', []) or []
        
        # Safety check for empty content
        if not title and not description:
            return {
                'article_id': article.get('article_id', ''),
                'title': 'No title available',
                'sentiment_score': 0.0,
                'sentiment_label': 'NEUTRAL',
                'sentiment_emoji': '📊',
                'confidence': 0,
                'error': 'No text content available'
            }
        
        try:
            # Combine text for analysis
            full_text = f"{title} {description}".strip()
            keywords_text = ' '.join(keywords) if keywords else ''
            
            # TextBlob sentiment analysis
            title_blob = TextBlob(title) if title else TextBlob("")
            desc_blob = TextBlob(description) if description else TextBlob("")
            full_blob = TextBlob(full_text) if full_text else TextBlob("neutral")
            
            # Calculate weighted sentiment scores
            title_sentiment = title_blob.sentiment.polarity * self.title_weight if title else 0
            desc_sentiment = desc_blob.sentiment.polarity * self.description_weight if description else 0
            
            # Keyword-based sentiment
            keyword_sentiment = self._analyze_keywords(keywords_text)
            
            # Combined weighted score
            active_weights = 0
            if title: active_weights += self.title_weight
            if description: active_weights += self.description_weight
            if keywords_text: active_weights += self.keyword_weight
            
            total_weight = max(active_weights, 1.0)
            weighted_sentiment = (
                title_sentiment + 
                desc_sentiment + 
                (keyword_sentiment * self.keyword_weight)
            ) / total_weight
            
            # Subjectivity (confidence) score
            subjectivity = full_blob.sentiment.subjectivity
            
        except Exception as e:
            # Fallback for any TextBlob errors
            weighted_sentiment = 0.0
            subjectivity = 0.0
        
        # Classify sentiment
        if weighted_sentiment > 0.1:
            sentiment_label = 'POSITIVE'
            emoji = '📈'
        elif weighted_sentiment < -0.1:
            sentiment_label = 'NEGATIVE'  
            emoji = '📉'
        else:
            sentiment_label = 'NEUTRAL'
            emoji = '📊'
        
        # Confidence level
        confidence = min(100, int((abs(weighted_sentiment) + subjectivity) * 100))
        
        return {
            'article_id': article.get('article_id', ''),
            'title': title or 'No title',
            'source': article.get('source_name', 'Unknown'),
            'pub_date': article.get('pubDate', ''),
            'url': article.get('link', ''),
            'sentiment_score': round(weighted_sentiment, 3),
            'sentiment_label': sentiment_label,
            'sentiment_emoji': emoji,
            'confidence': confidence,
            'subjectivity': round(subjectivity, 3),
            'positive_keywords_found': self._find_keywords(full_text, self.positive_keywords),
            'negative_keywords_found': self._find_keywords(full_text, self.negative_keywords),
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def _analyze_keywords(self, text: str) -> float:
        """Analyze sentiment based on financial keywords"""
        if not text:
            return 0.0
            
        text_lower = text.lower()
        
        positive_count = sum(1 for word in self.positive_keywords if word in text_lower)
        negative_count = sum(1 for word in self.negative_keywords if word in text_lower)
        
        if positive_count == 0 and negative_count == 0:
            return 0.0
        
        total_sentiment_words = positive_count + negative_count
        sentiment_score = (positive_count - negative_count) / total_sentiment_words
        
        return sentiment_score
    
    def _find_keywords(self, text: str, keyword_set: set) -> List[str]:
        """Find matching keywords in text"""
        if not text:
            return []
        
        text_lower = text.lower()
        found_keywords = [word for word in keyword_set if word in text_lower]
        return found_keywords

def check_news_deduplication(user_client, article_id: str, stock_query: str) -> bool:
    """
    Check if news article has already been processed for any user
    Returns True if already processed, False if new
    """
    try:
        result = user_client.table('processed_news_articles')\
            .select('id')\
            .eq('article_id', article_id)\
            .eq('stock_query', stock_query)\
            .execute()
        
        return len(result.data) > 0
    except Exception as e:
        print(f"Error checking news deduplication: {e}")
        return False

def mark_news_as_processed(user_client, article: Dict, stock_query: str, user_ids: List[str]):
    """
    Mark news article as processed to prevent future duplicates
    """
    try:
        user_client.table('processed_news_articles').insert({
            'article_id': article.get('article_id', ''),
            'title': article.get('title', ''),
            'url': article.get('link', ''),
            'source_name': article.get('source_name', ''),
            'pub_date': article.get('pubDate'),
            'stock_query': stock_query,
            'sent_to_users': user_ids
        }).execute()
    except Exception as e:
        print(f"Error marking news as processed: {e}")

def save_sentiment_analysis(user_client, user_id: str, stock_name: str, bse_code: str, sentiment_report: Dict):
    """
    Save sentiment analysis results to database
    """
    try:
        user_client.table('news_sentiment_analysis').insert({
            'user_id': user_id,
            'stock_name': stock_name,
            'bse_code': bse_code,
            'overall_sentiment': sentiment_report.get('overall_sentiment', 'NEUTRAL'),
            'sentiment_score': sentiment_report.get('sentiment_score', 0.0),
            'confidence': sentiment_report.get('confidence', 0),
            'total_articles': sentiment_report.get('total_articles', 0),
            'positive_articles': sentiment_report.get('positive_articles', 0),
            'negative_articles': sentiment_report.get('negative_articles', 0),
            'neutral_articles': sentiment_report.get('neutral_articles', 0),
            'news_source': 'NewsData.io',
            'api_requests_used': sentiment_report.get('api_requests_used', 1)
        }).execute()
    except Exception as e:
        print(f"Error saving sentiment analysis: {e}")

def check_user_sentiment_preferences(user_client, user_id: str, stock_name: str) -> Dict:
    """
    Check if user has sentiment analysis enabled for this stock
    """
    try:
        result = user_client.table('user_sentiment_preferences')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('stock_name', stock_name)\
            .execute()
        
        if result.data:
            return result.data[0]
        else:
            # Default preferences if not set
            return {
                'sentiment_enabled': True,
                'min_confidence_threshold': 40,
                'notification_frequency': 'real_time'
            }
    except Exception as e:
        print(f"Error checking sentiment preferences: {e}")
        return {'sentiment_enabled': True, 'min_confidence_threshold': 40}

def format_news_sentiment_telegram_message(stock_name: str, articles_analyzed: List[Dict], sentiment_summary: Dict) -> str:
    """
    Format news sentiment analysis for Telegram with links and summaries
    """
    if not articles_analyzed:
        return f"📰 No recent news found for {stock_name}"
    
    # Header
    message = f"""🎯 NEWS SENTIMENT: {stock_name}
🕐 {datetime.now().strftime('%Y-%m-%d %H:%M IST')}

{sentiment_summary.get('overall_emoji', '📊')} OVERALL: {sentiment_summary.get('overall_sentiment', 'NEUTRAL')}
📊 Score: {sentiment_summary.get('sentiment_score', 0)} | Confidence: {sentiment_summary.get('confidence', 0)}%
📰 Articles: {len(articles_analyzed)}

"""
    
    # Top articles with sentiment
    message += "📰 LATEST NEWS:\n\n"
    
    # Sort by sentiment score magnitude (most significant first)
    sorted_articles = sorted(
        articles_analyzed[:5],  # Top 5 articles
        key=lambda x: abs(x.get('sentiment_score', 0)), 
        reverse=True
    )
    
    for i, article in enumerate(sorted_articles, 1):
        title = article.get('title', 'No title')
        # Truncate long titles
        if len(title) > 60:
            title = title[:60] + '...'
            
        emoji = article.get('sentiment_emoji', '📊')
        score = article.get('sentiment_score', 0)
        source = article.get('source', 'Unknown')
        url = article.get('url', '')
        
        # Format article entry
        message += f"{i}. {emoji} {title}\n"
        message += f"   📊 {score} | 🏢 {source}\n"
        if url:
            message += f"   🔗 {url}\n"
        message += "\n"
    
    return message

def send_news_sentiment_alerts(user_client, user_id: str, monitored_scrips: List[Dict], telegram_recipients: List[Dict]) -> int:
    """
    Main function for news sentiment monitoring - follows existing BSE pattern
    
    Args:
        user_client: Supabase client
        user_id: User ID 
        monitored_scrips: List of user's monitored stocks
        telegram_recipients: List of user's Telegram recipients
        
    Returns:
        Number of messages sent
    """
    messages_sent = 0
    
    # Get NewsData.io API key
    api_key = os.environ.get('NEWSDATA_API_KEY')
    if not api_key:
        print("ERROR: NEWSDATA_API_KEY not configured")
        return 0
    
    # Initialize clients
    news_client = NewsDataAPIClient(api_key)
    sentiment_analyzer = StockSentimentAnalyzer()
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Starting sentiment analysis for user {user_id} with {len(monitored_scrips)} stocks")
    
    for scrip in monitored_scrips:
        company_name = scrip.get('company_name', '')
        bse_code = scrip.get('bse_code', '')
        
        if not company_name:
            continue
        
        # Check user preferences for this stock
        preferences = check_user_sentiment_preferences(user_client, user_id, company_name)
        if not preferences.get('sentiment_enabled', True):
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Sentiment analysis disabled for {company_name}")
            continue
        
        try:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Fetching news for {company_name}")
            
            # Fetch news for this stock
            news_result = news_client.fetch_stock_news(company_name, size=10)
            
            if not news_result.get('success'):
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: Failed to fetch news for {company_name}: {news_result.get('error')}")
                continue
            
            articles = news_result.get('articles', [])
            if not articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No articles found for {company_name}")
                continue
            
            # Filter out already processed articles
            new_articles = []
            for article in articles:
                article_id = article.get('article_id', '')
                if not check_news_deduplication(user_client, article_id, company_name):
                    new_articles.append(article)
            
            if not new_articles:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: No new articles for {company_name} (all already processed)")
                continue
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"NEWS: Found {len(new_articles)} new articles for {company_name}")
            
            # Perform sentiment analysis
            analyzed_articles = []
            sentiment_scores = []
            
            for article in new_articles:
                analysis = sentiment_analyzer.analyze_article_sentiment(article)
                analyzed_articles.append(analysis)
                sentiment_scores.append(analysis['sentiment_score'])
            
            # Calculate aggregate sentiment
            if sentiment_scores:
                avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                sentiment_counts = Counter([art['sentiment_label'] for art in analyzed_articles])
                
                # Determine overall sentiment
                if avg_sentiment > 0.1:
                    overall_sentiment = 'POSITIVE'
                    overall_emoji = '📈'
                elif avg_sentiment < -0.1:
                    overall_sentiment = 'NEGATIVE'
                    overall_emoji = '📉'
                else:
                    overall_sentiment = 'NEUTRAL'
                    overall_emoji = '📊'
                
                confidence = min(100, int((abs(avg_sentiment) * 50) + 50))
                
                sentiment_summary = {
                    'overall_sentiment': overall_sentiment,
                    'overall_emoji': overall_emoji,
                    'sentiment_score': round(avg_sentiment, 3),
                    'confidence': confidence,
                    'total_articles': len(analyzed_articles),
                    'positive_articles': sentiment_counts.get('POSITIVE', 0),
                    'negative_articles': sentiment_counts.get('NEGATIVE', 0),
                    'neutral_articles': sentiment_counts.get('NEUTRAL', 0),
                    'api_requests_used': 1
                }
                
                # Check confidence threshold
                min_confidence = preferences.get('min_confidence_threshold', 40)
                if confidence < min_confidence:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"NEWS: Skipping {company_name} - confidence {confidence}% below threshold {min_confidence}%")
                    continue
                
                # Format and send Telegram message
                message = format_news_sentiment_telegram_message(company_name, analyzed_articles, sentiment_summary)
                
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
                                print(f"NEWS: Sent sentiment alert for {company_name} to {chat_id}")
                        else:
                            print(f"NEWS: Telegram API error for {chat_id}: {response.text}")
                            
                    except Exception as send_error:
                        print(f"NEWS: Error sending to {chat_id}: {send_error}")
                
                # Mark articles as processed
                article_ids = [art['article_id'] for art in analyzed_articles]
                for article in new_articles:
                    mark_news_as_processed(user_client, article, company_name, [user_id])
                
                # Save sentiment analysis to database
                save_sentiment_analysis(user_client, user_id, company_name, bse_code, sentiment_summary)
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"NEWS: Completed sentiment analysis for {company_name} - {overall_sentiment} ({confidence}%)")
            
        except Exception as e:
            print(f"NEWS: Error processing {company_name}: {e}")
            continue
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"NEWS: Completed sentiment analysis for user {user_id}, sent {messages_sent} messages")
    
    return messages_sent

# Function to be called from unified cron system
def send_news_sentiment_monitoring(user_client, user_id: str, monitored_scrips: List[Dict], telegram_recipients: List[Dict]) -> int:
    """
    Wrapper function that matches the existing BSE monitoring pattern
    This function will be called from the unified cron system
    """
    return send_news_sentiment_alerts(user_client, user_id, monitored_scrips, telegram_recipients)