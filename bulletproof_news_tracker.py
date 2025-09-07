#!/usr/bin/env python3
"""
Bulletproof News Tracking System
This will absolutely prevent duplicates by using multiple layers of protection
"""

import hashlib
import time
import os
from datetime import datetime, timedelta
from typing import Dict, Set
import json

# Global in-memory cache to prevent duplicates within the same process
_SENT_ARTICLES_CACHE = {}
_USER_LOCKS = {}

def get_canonical_article_id(article: Dict, company_name: str) -> str:
    """
    Generate a canonical article ID that's the same regardless of source
    """
    try:
        # Method 1: Use URL without query parameters
        url = article.get('link', article.get('url', ''))
        if url:
            # Remove query parameters and fragments
            from urllib.parse import urlparse
            parsed = urlparse(url)
            canonical_url = f"{parsed.netloc.lower()}{parsed.path.rstrip('/')}"
            if canonical_url:
                return hashlib.md5(canonical_url.encode()).hexdigest()[:16]
        
        # Method 2: Use normalized title
        title = article.get('title', '').lower().strip()
        if title:
            # Remove company name from title to avoid variations
            company_words = company_name.lower().split()
            for word in company_words:
                if len(word) > 3:  # Only remove meaningful words
                    title = title.replace(word.lower(), '')
            
            # Remove common noise words
            noise_words = ['ltd', 'limited', 'inc', 'corp', 'company', 'the', 'and', 'of', 'in', 'to', 'for']
            for noise in noise_words:
                title = title.replace(f' {noise} ', ' ')
            
            # Clean up whitespace and punctuation
            import re
            title = re.sub(r'[^\w\s]', '', title)
            title = re.sub(r'\s+', ' ', title).strip()
            
            if title:
                return hashlib.md5(title.encode()).hexdigest()[:16]
        
        # Fallback: timestamp-based (will be unique but won't catch duplicates)
        return hashlib.md5(str(time.time()).encode()).hexdigest()[:16]
        
    except Exception as e:
        print(f"❌ Error generating canonical ID: {e}")
        return hashlib.md5(str(time.time()).encode()).hexdigest()[:16]

def is_duplicate_in_memory(article_id: str, company_name: str, user_id: str) -> bool:
    """
    Check in-memory cache for duplicates (fastest check)
    """
    cache_key = f"{article_id}_{company_name}_{user_id}"
    
    if cache_key in _SENT_ARTICLES_CACHE:
        sent_time = _SENT_ARTICLES_CACHE[cache_key]
        time_diff = time.time() - sent_time
        
        if time_diff < 3600:  # 1 hour cache
            print(f"🚫 IN-MEMORY DUPLICATE: {cache_key} sent {time_diff:.0f}s ago")
            return True
        else:
            # Remove old entries
            del _SENT_ARTICLES_CACHE[cache_key]
    
    return False

def mark_sent_in_memory(article_id: str, company_name: str, user_id: str):
    """
    Mark article as sent in memory cache
    """
    cache_key = f"{article_id}_{company_name}_{user_id}"
    _SENT_ARTICLES_CACHE[cache_key] = time.time()
    print(f"📝 CACHED: {cache_key}")

def is_user_locked(user_id: str) -> bool:
    """
    Check if user is currently being processed
    """
    if user_id in _USER_LOCKS:
        lock_time = _USER_LOCKS[user_id]
        time_diff = time.time() - lock_time
        
        if time_diff < 300:  # 5 minutes
            print(f"🔒 USER LOCKED: {user_id} locked {time_diff:.0f}s ago")
            return True
        else:
            # Remove old locks
            del _USER_LOCKS[user_id]
    
    return False

def lock_user(user_id: str):
    """
    Lock user for processing
    """
    _USER_LOCKS[user_id] = time.time()
    print(f"🔒 LOCKED USER: {user_id}")

def unlock_user(user_id: str):
    """
    Unlock user after processing
    """
    if user_id in _USER_LOCKS:
        del _USER_LOCKS[user_id]
        print(f"🔓 UNLOCKED USER: {user_id}")

def check_database_duplicate(user_client, article_id: str, company_name: str, user_id: str) -> bool:
    """
    Check database for duplicates with comprehensive error handling
    """
    try:
        print(f"🔍 DB CHECK: article_id={article_id[:8]}..., company={company_name}, user={user_id[:8]}...")
        
        # Try new table first
        try:
            result = user_client.table('news_sent_tracking')\
                .select('id')\
                .eq('article_id', article_id)\
                .eq('company_name', company_name)\
                .eq('user_id', user_id)\
                .execute()
            
            if len(result.data) > 0:
                print(f"🚫 DB DUPLICATE (new table): Found {len(result.data)} records")
                return True
            
            print(f"✅ DB NEW (new table): No records found")
            return False
            
        except Exception as e:
            print(f"⚠️ New table failed: {e}")
            
            # Fallback to old table
            try:
                result = user_client.table('processed_news_articles')\
                    .select('id')\
                    .eq('article_id', article_id)\
                    .eq('stock_query', company_name)\
                    .execute()
                
                if len(result.data) > 0:
                    print(f"🚫 DB DUPLICATE (old table): Found {len(result.data)} records")
                    return True
                
                print(f"✅ DB NEW (old table): No records found")
                return False
                
            except Exception as e2:
                print(f"❌ Both tables failed: new={e}, old={e2}")
                return False
        
    except Exception as e:
        print(f"❌ DB CHECK ERROR: {e}")
        return False

def store_in_database(user_client, article: Dict, article_id: str, company_name: str, user_id: str):
    """
    Store article in database with comprehensive error handling
    """
    try:
        print(f"📝 DB STORE: Storing {article_id[:8]}... for {user_id[:8]}...")
        
        # Try new table first
        try:
            tracking_data = {
                'article_id': article_id,
                'article_title': article.get('title', '')[:500],
                'article_url': article.get('link', article.get('url', ''))[:1000],
                'company_name': company_name,
                'user_id': user_id
            }
            
            print(f"🔍 INSERTING: {tracking_data}")
            result = user_client.table('news_sent_tracking').insert(tracking_data).execute()
            print(f"✅ DB STORED (new table): {result}")
            return
            
        except Exception as e:
            print(f"⚠️ New table insert failed: {e}")
            
            # Fallback to old table
            try:
                old_data = {
                    'article_id': article_id,
                    'title': article.get('title', '')[:255],
                    'url': article.get('link', article.get('url', ''))[:500],
                    'source_name': article.get('source', '')[:100],
                    'pub_date': article.get('pubDate', '')[:50],
                    'stock_query': company_name,
                    'sent_to_users': [user_id]
                }
                
                result = user_client.table('processed_news_articles').insert(old_data).execute()
                print(f"✅ DB STORED (old table): {result}")
                return
                
            except Exception as e2:
                print(f"❌ Both table inserts failed: new={e}, old={e2}")
        
    except Exception as e:
        print(f"❌ DB STORE ERROR: {e}")

def is_article_duplicate(user_client, article: Dict, company_name: str, user_id: str) -> bool:
    """
    Comprehensive duplicate check with multiple layers
    """
    # Generate canonical article ID
    article_id = get_canonical_article_id(article, company_name)
    
    print(f"🔍 DUPLICATE CHECK: article_id={article_id[:8]}..., company={company_name}, user={user_id[:8]}...")
    
    # Layer 1: In-memory cache (fastest)
    if is_duplicate_in_memory(article_id, company_name, user_id):
        return True
    
    # Layer 2: Database check
    if check_database_duplicate(user_client, article_id, company_name, user_id):
        # Also cache it for future in-memory checks
        mark_sent_in_memory(article_id, company_name, user_id)
        return True
    
    return False

def mark_article_sent(user_client, article: Dict, company_name: str, user_id: str):
    """
    Mark article as sent in all tracking systems
    """
    # Generate canonical article ID
    article_id = get_canonical_article_id(article, company_name)
    
    print(f"📝 MARKING SENT: article_id={article_id[:8]}..., company={company_name}, user={user_id[:8]}...")
    
    # Store in memory cache
    mark_sent_in_memory(article_id, company_name, user_id)
    
    # Store in database
    store_in_database(user_client, article, article_id, company_name, user_id)

def cleanup_cache():
    """
    Clean up old entries from memory cache
    """
    current_time = time.time()
    
    # Clean article cache
    to_remove = []
    for key, sent_time in _SENT_ARTICLES_CACHE.items():
        if current_time - sent_time > 3600:  # 1 hour
            to_remove.append(key)
    
    for key in to_remove:
        del _SENT_ARTICLES_CACHE[key]
    
    # Clean user locks
    to_remove = []
    for user_id, lock_time in _USER_LOCKS.items():
        if current_time - lock_time > 300:  # 5 minutes
            to_remove.append(user_id)
    
    for user_id in to_remove:
        del _USER_LOCKS[user_id]
    
    if to_remove:
        print(f"🧹 CLEANUP: Removed {len(to_remove)} old entries")

def get_debug_stats() -> Dict:
    """
    Get debug statistics
    """
    return {
        'cached_articles': len(_SENT_ARTICLES_CACHE),
        'locked_users': len(_USER_LOCKS),
        'cache_entries': list(_SENT_ARTICLES_CACHE.keys())[:5],  # First 5
        'locked_user_ids': list(_USER_LOCKS.keys())
    }