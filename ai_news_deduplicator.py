#!/usr/bin/env python3
"""
AI-Powered News Deduplication Service
Uses Google Gemini AI to intelligently identify and merge duplicate/similar news articles

Features:
1. Semantic similarity detection beyond simple title matching
2. Content clustering and representative article selection
3. Merged sentiment analysis for similar articles
4. Preserves source attribution while eliminating redundancy
"""

import os
import google.generativeai as genai
from typing import List, Dict, Optional, Tuple
import json
import hashlib
from datetime import datetime
import time

class AINewsDeduplicator:
    """AI-powered news deduplication using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.environ.get('GOOGLE_API_KEY')
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
        else:
            self.model = None
            print("⚠️ Google API key not found - falling back to simple deduplication")
    
    def deduplicate_articles(self, articles: List[Dict]) -> Dict:
        """
        Main deduplication function
        Returns deduplicated articles with clustering information
        """
        if not articles:
            return {
                'deduplicated_articles': [],
                'duplicate_clusters': [],
                'stats': {'original_count': 0, 'deduplicated_count': 0, 'duplicates_removed': 0}
            }
        
        if len(articles) <= 2:
            # Skip AI processing for very small sets
            return {
                'deduplicated_articles': articles,
                'duplicate_clusters': [],
                'stats': {'original_count': len(articles), 'deduplicated_count': len(articles), 'duplicates_removed': 0}
            }
        
        if self.model and len(articles) >= 3:
            # Use AI deduplication for 3+ articles
            return self._ai_deduplicate(articles)
        else:
            # Fallback to simple deduplication
            return self._simple_deduplicate(articles)
    
    def _ai_deduplicate(self, articles: List[Dict]) -> Dict:
        """AI-powered semantic deduplication using Gemini"""
        try:
            # Prepare articles for AI analysis
            article_summaries = []
            for i, article in enumerate(articles):
                summary = {
                    'id': i,
                    'title': article.get('title', ''),
                    'description': article.get('description', '')[:300],  # Limit for token efficiency
                    'source': article.get('source', 'Unknown'),
                    'url': article.get('url', ''),
                    'timestamp': article.get('pubDate', '')
                }
                article_summaries.append(summary)
            
            # Create AI prompt for deduplication
            prompt = self._create_deduplication_prompt(article_summaries)
            
            # Get AI response
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                # Parse AI response
                clusters = self._parse_ai_response(response.text, articles)
                
                # Generate deduplicated articles from clusters
                deduplicated_articles = self._create_deduplicated_articles(clusters, articles)
                
                stats = {
                    'original_count': len(articles),
                    'deduplicated_count': len(deduplicated_articles),
                    'duplicates_removed': len(articles) - len(deduplicated_articles),
                    'clusters_found': len(clusters),
                    'method': 'ai_gemini'
                }
                
                return {
                    'deduplicated_articles': deduplicated_articles,
                    'duplicate_clusters': clusters,
                    'stats': stats
                }
            else:
                # AI failed, fallback to simple deduplication
                return self._simple_deduplicate(articles)
                
        except Exception as e:
            print(f"AI deduplication error: {e}")
            # Fallback to simple deduplication
            return self._simple_deduplicate(articles)
    
    def _create_deduplication_prompt(self, article_summaries: List[Dict]) -> str:
        """Create AI prompt for news deduplication"""
        
        articles_text = ""
        for article in article_summaries:
            articles_text += f"""
Article {article['id']}:
Title: {article['title']}
Description: {article['description']}
Source: {article['source']}
---"""
        
        prompt = f"""
You are a financial news analyst tasked with identifying duplicate or very similar news articles.

ARTICLES TO ANALYZE:
{articles_text}

TASK: Group articles that report the same news event or story, even if worded differently.

RULES:
1. Articles about the same company event (earnings, announcements, deals) should be grouped together
2. Different perspectives on the same news event should be grouped
3. Only group articles if they are clearly about the same specific event/story
4. Minor variations in wording don't make articles different if the core event is the same
5. Different types of news (price movements vs earnings vs announcements) should NOT be grouped

OUTPUT FORMAT (JSON only, no other text):
{{
  "clusters": [
    {{
      "primary_article_id": 0,
      "related_article_ids": [1, 2],
      "reason": "All report OLA Electric quarterly earnings announcement",
      "confidence": 0.95
    }},
    {{
      "primary_article_id": 3,
      "related_article_ids": [],
      "reason": "Unique story about market analysis",
      "confidence": 1.0
    }}
  ]
}}

IMPORTANT: 
- primary_article_id is the best representative article for the cluster
- related_article_ids are the duplicates/similar articles to be merged
- confidence should be 0.8+ for grouping articles
- Return JSON only, no explanations or other text
"""
        return prompt
    
    def _parse_ai_response(self, response_text: str, articles: List[Dict]) -> List[Dict]:
        """Parse AI response and extract clusters"""
        try:
            # Clean response text
            response_text = response_text.strip()
            
            # Try to extract JSON from response
            if '```json' in response_text:
                # Extract JSON from code block
                json_start = response_text.find('```json') + 7
                json_end = response_text.find('```', json_start)
                json_text = response_text[json_start:json_end].strip()
            elif response_text.startswith('{'):
                # Response is direct JSON
                json_text = response_text
            else:
                # Try to find JSON-like content
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start >= 0 and json_end > json_start:
                    json_text = response_text[json_start:json_end]
                else:
                    raise ValueError("No JSON found in response")
            
            # Parse JSON
            ai_result = json.loads(json_text)
            clusters = []
            
            for cluster_data in ai_result.get('clusters', []):
                primary_id = cluster_data.get('primary_article_id')
                related_ids = cluster_data.get('related_article_ids', [])
                confidence = cluster_data.get('confidence', 0.0)
                reason = cluster_data.get('reason', '')
                
                # Validate cluster data
                if (primary_id is not None and 
                    0 <= primary_id < len(articles) and 
                    confidence >= 0.8 and
                    all(0 <= rid < len(articles) for rid in related_ids)):
                    
                    cluster = {
                        'primary_article_id': primary_id,
                        'related_article_ids': related_ids,
                        'all_article_ids': [primary_id] + related_ids,
                        'reason': reason,
                        'confidence': confidence,
                        'cluster_size': 1 + len(related_ids)
                    }
                    clusters.append(cluster)
            
            return clusters
            
        except Exception as e:
            print(f"Error parsing AI response: {e}")
            return []
    
    def _create_deduplicated_articles(self, clusters: List[Dict], articles: List[Dict]) -> List[Dict]:
        """Create deduplicated articles from clusters"""
        deduplicated = []
        used_article_ids = set()
        
        # Process clusters (grouped articles)
        for cluster in clusters:
            primary_id = cluster['primary_article_id']
            related_ids = cluster['related_article_ids']
            
            if primary_id not in used_article_ids:
                # Get primary article
                primary_article = articles[primary_id].copy()
                
                # Enhance with cluster information
                if related_ids:
                    # Collect all sources
                    all_sources = [primary_article.get('source', 'Unknown')]
                    all_urls = [primary_article.get('url', '')]
                    
                    for rid in related_ids:
                        if rid < len(articles):
                            related_article = articles[rid]
                            source = related_article.get('source', 'Unknown')
                            url = related_article.get('url', '')
                            
                            if source not in all_sources:
                                all_sources.append(source)
                            if url and url not in all_urls:
                                all_urls.append(url)
                    
                    # Update primary article with merged information
                    primary_article['merged_sources'] = all_sources
                    primary_article['merged_urls'] = all_urls
                    primary_article['duplicate_count'] = len(related_ids)
                    primary_article['cluster_reason'] = cluster.get('reason', '')
                    primary_article['cluster_confidence'] = cluster.get('confidence', 0.0)
                    primary_article['is_clustered'] = True
                    
                    # Update source display
                    if len(all_sources) > 1:
                        primary_article['source'] = f"{all_sources[0]} (+{len(all_sources)-1} more)"
                
                deduplicated.append(primary_article)
                
                # Mark all cluster articles as used
                used_article_ids.add(primary_id)
                used_article_ids.update(related_ids)
        
        # Add standalone articles (not in any cluster)
        for i, article in enumerate(articles):
            if i not in used_article_ids:
                standalone_article = article.copy()
                standalone_article['is_clustered'] = False
                standalone_article['duplicate_count'] = 0
                deduplicated.append(standalone_article)
        
        return deduplicated
    
    def _simple_deduplicate(self, articles: List[Dict]) -> Dict:
        """Simple title-based deduplication fallback"""
        deduplicated = []
        seen_titles = set()
        
        for article in articles:
            title = article.get('title', '').lower().strip()
            title_key = title[:50] if title else str(len(deduplicated))
            
            if title_key not in seen_titles and title:
                seen_titles.add(title_key)
                article_copy = article.copy()
                article_copy['is_clustered'] = False
                article_copy['duplicate_count'] = 0
                deduplicated.append(article_copy)
        
        stats = {
            'original_count': len(articles),
            'deduplicated_count': len(deduplicated),
            'duplicates_removed': len(articles) - len(deduplicated),
            'method': 'simple_title_matching'
        }
        
        return {
            'deduplicated_articles': deduplicated,
            'duplicate_clusters': [],
            'stats': stats
        }

# Integration function for sentiment analysis
def ai_deduplicate_news_articles(articles: List[Dict]) -> Dict:
    """
    Main function to deduplicate news articles using AI
    Compatible with existing sentiment analysis pipeline
    """
    deduplicator = AINewsDeduplicator()
    return deduplicator.deduplicate_articles(articles)

if __name__ == "__main__":
    # Test the AI deduplicator
    print("🧪 Testing AI News Deduplicator...")
    
    # Sample duplicate articles for testing
    test_articles = [
        {
            'title': 'Ola Electric shares surge 12% on strong quarterly results',
            'description': 'Ola Electric Mobility shares jumped 12% today after the company reported strong Q2 results with revenue growth of 25%.',
            'source': 'Economic Times',
            'url': 'https://economictimes.indiatimes.com/ola1'
        },
        {
            'title': 'OLA Electric stock rises 10% after impressive Q2 earnings',
            'description': 'OLA Electric stock gained 10% in morning trade following the announcement of impressive Q2 earnings with 25% revenue increase.',
            'source': 'Business Standard',
            'url': 'https://business-standard.com/ola2'
        },
        {
            'title': 'Ola Electric Mobility reports 25% revenue growth in Q2',
            'description': 'Electric vehicle maker Ola Electric announced its Q2 results showing 25% revenue growth compared to previous quarter.',
            'source': 'MoneyControl',
            'url': 'https://moneycontrol.com/ola3'
        },
        {
            'title': 'Reliance Industries announces new retail expansion',
            'description': 'Reliance Industries announced plans to expand its retail operations with 500 new stores across India.',
            'source': 'Financial Express',
            'url': 'https://financialexpress.com/reliance1'
        }
    ]
    
    deduplicator = AINewsDeduplicator()
    result = deduplicator.deduplicate_articles(test_articles)
    
    print(f"✅ Original articles: {result['stats']['original_count']}")
    print(f"✅ After deduplication: {result['stats']['deduplicated_count']}")
    print(f"✅ Duplicates removed: {result['stats']['duplicates_removed']}")
    print(f"✅ Method used: {result['stats'].get('method', 'unknown')}")
    
    if result['duplicate_clusters']:
        print(f"\n📊 Clusters found:")
        for i, cluster in enumerate(result['duplicate_clusters'], 1):
            print(f"  {i}. {cluster['reason']} (confidence: {cluster['confidence']})")
    
    print(f"\n📰 Deduplicated articles:")
    for i, article in enumerate(result['deduplicated_articles'], 1):
        title = article['title'][:60] + '...' if len(article['title']) > 60 else article['title']
        clustered = "🔗" if article.get('is_clustered') else "📄"
        dup_count = article.get('duplicate_count', 0)
        source_info = f" (+{dup_count} similar)" if dup_count > 0 else ""
        print(f"  {i}. {clustered} {title}{source_info}")
        print(f"     Source: {article['source']}")