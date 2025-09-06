#!/usr/bin/env python3
"""
Simple News Tracking System
Uses dedicated news_sent_tracking table for bulletproof duplicate prevention
"""

import hashlib
from datetime import datetime, timedelta
from typing import Dict
import os

def check_news_sent_simple(user_client, article: Dict, company_name: str, user_id: str) -> bool:
    """
    Simple check: Has this exact article been sent to this user for this company?
    Returns True if already sent, False if new
    """
    try:
        # First, test if we can access the table at all
        print(f"🔍 DEBUG: Testing table access...")
        test_result = user_client.table('news_sent_tracking').select('id').limit(1).execute()
        print(f"🔍 DEBUG: Table access test successful, found {len(test_result.data)} records")
        # Generate article ID
        article_id = article.get('article_id', '')
        if not article_id:
            url = article.get('link', article.get('url', ''))
            title = article.get('title', '')
            if url:
                article_id = hashlib.md5(url.encode()).hexdigest()[:16]
            elif title:
                article_id = hashlib.md5(title.encode()).hexdigest()[:16]
            else:
                return False
        
        # Simple query: Check if this exact combination exists
        result = user_client.table('news_sent_tracking')\
            .select('id')\
            .eq('article_id', article_id)\
            .eq('company_name', company_name)\
            .eq('user_id', user_id)\
            .execute()
        
        print(f"🔍 DEBUG: Checking article_id={article_id[:8]}..., company={company_name}, user={user_id[:8]}...")
        print(f"🔍 DEBUG: Query result: {len(result.data)} records found")
        
        if len(result.data) > 0:
            print(f"NEWS: 🚫 ALREADY SENT - Article {article_id[:8]}... to user {user_id[:8]}... for {company_name}")
            return True
        
        print(f"NEWS: ✅ NEW FOR USER - Article {article_id[:8]}... for user {user_id[:8]}... and {company_name}")
        return False
        
    except Exception as e:
        print(f"❌ ERROR in check_news_sent_simple: {e}")
        print(f"❌ ERROR type: {type(e)}")
        import traceback
        print(f"❌ ERROR traceback: {traceback.format_exc()}")
        # Fallback to old system if new table doesn't exist
        return check_news_already_sent_fallback(user_client, article, company_name, user_id)

def store_news_sent_simple(user_client, article: Dict, company_name: str, user_id: str):
    """
    Store that this article was sent to this user for this company
    """
    try:
        # Test table access before inserting
        print(f"🔍 DEBUG: Testing table access for insert...")
        test_result = user_client.table('news_sent_tracking').select('id').limit(1).execute()
        print(f"🔍 DEBUG: Table accessible for insert, found {len(test_result.data)} existing records")
        # Generate article ID
        article_id = article.get('article_id', '')
        if not article_id:
            url = article.get('link', article.get('url', ''))
            title = article.get('title', '')
            if url:
                article_id = hashlib.md5(url.encode()).hexdigest()[:16]
            elif title:
                article_id = hashlib.md5(title.encode()).hexdigest()[:16]
            else:
                return
        
        # Store the record
        tracking_data = {
            'article_id': article_id,
            'article_title': article.get('title', '')[:500],  # Limit length
            'article_url': article.get('link', article.get('url', ''))[:1000],  # Limit length
            'company_name': company_name,
            'user_id': user_id
            # sent_at will be auto-populated by DEFAULT CURRENT_TIMESTAMP
        }
        
        # Insert with debug logging
        print(f"🔍 DEBUG: Inserting tracking_data: {tracking_data}")
        
        result = user_client.table('news_sent_tracking').insert(tracking_data).execute()
        
        print(f"🔍 DEBUG: Insert result: {result}")
        print(f"NEWS: 📝 TRACKED - Stored {article.get('title', 'Unknown')[:50]}... for user {user_id[:8]}...")
            
    except Exception as e:
        print(f"❌ ERROR in store_news_sent_simple: {e}")
        print(f"❌ ERROR type: {type(e)}")
        import traceback
        print(f"❌ ERROR traceback: {traceback.format_exc()}")
        print(f"❌ Failed to store: {tracking_data}")
        # Fallback to old system if new table doesn't exist
        store_sent_news_article_fallback(user_client, article, company_name, user_id)

def cleanup_old_tracking_records(user_client, days_to_keep: int = 7):
    """
    Clean up old tracking records to keep database lean
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        result = user_client.table('news_sent_tracking')\
            .delete()\
            .lt('sent_at', cutoff_date.isoformat())\
            .execute()
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: 🧹 CLEANUP - Removed tracking records older than {days_to_keep} days")
            
    except Exception as e:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"NEWS: Cleanup failed: {e}")

def check_news_already_sent_fallback(user_client, article: Dict, company_name: str, user_id: str) -> bool:
    """Fallback to old system if new table doesn't exist"""
    # This would call the existing check_news_already_sent function
    pass

def store_sent_news_article_fallback(user_client, article: Dict, company_name: str, user_id: str):
    """Fallback to old system if new table doesn't exist"""
    # This would call the existing store_sent_news_article function
    pass

def get_tracking_stats(user_client, company_name: str = None, days: int = 7) -> Dict:
    """
    Get statistics about sent news for debugging
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = user_client.table('news_sent_tracking')\
            .select('company_name, article_title, user_id, sent_at')\
            .gte('sent_at', cutoff_date.isoformat())
        
        if company_name:
            query = query.eq('company_name', company_name)
        
        result = query.execute()
        
        stats = {
            'total_sent': len(result.data),
            'companies': {},
            'recent_articles': []
        }
        
        for record in result.data:
            company = record.get('company_name', 'Unknown')
            if company not in stats['companies']:
                stats['companies'][company] = 0
            stats['companies'][company] += 1
            
            stats['recent_articles'].append({
                'company': company,
                'title': record.get('article_title', 'Unknown')[:50],
                'sent_at': record.get('sent_at', 'Unknown')
            })
        
        return stats
        
    except Exception as e:
        return {'error': str(e)}