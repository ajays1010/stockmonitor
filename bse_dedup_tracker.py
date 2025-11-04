#!/usr/bin/env python3
"""
BSE Announcement Deduplication Tracker
Enhanced duplicate prevention with memory-based tracking and time windows
"""

import os
import hashlib
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, Tuple
from collections import defaultdict
import weakref

class BSEAnnouncementTracker:
    """
    Enhanced BSE announcement tracker that prevents duplicates across:
    - Multiple cron runs within time windows
    - Multiple users receiving same announcement
    - Rapid re-fetches from BSE API
    """

    def __init__(self):
        # Memory-based tracking with time windows
        self._recent_announcements: Dict[str, float] = {}  # news_id -> timestamp
        self._user_announcements: Dict[str, Set[str]] = defaultdict(set)  # user_id -> set of news_ids
        self._announcement_hashes: Dict[str, float] = {}  # content_hash -> timestamp

        # Cleanup tracking
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5 minutes

        # Time windows (in seconds)
        self._global_duplicate_window = 7200  # 2 hours - prevent same announcement globally
        self._user_duplicate_window = 86400   # 24 hours - prevent per-user duplicates
        self._hash_duplicate_window = 3600    # 1 hour - prevent similar content
        self._rapid_refetch_window = 120      # 2 minutes - prevent rapid API refetch duplicates

        # Enable verbose logging
        self._verbose = os.environ.get('BSE_VERBOSE', '0') == '1'

        if self._verbose:
            print("🔍 BSE Dedup Tracker: Initialized with enhanced time windows")

    def _generate_content_hash(self, headline: str, company_name: str, ann_dt: str) -> str:
        """Generate a content-based hash for announcements to catch near-duplicates"""
        content = f"{headline}|{company_name}|{ann_dt}"
        # Normalize content for better matching
        content = content.lower().strip()
        # Remove extra whitespace and common variations
        content = ' '.join(content.split())
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def _cleanup_old_entries(self):
        """Clean up old entries to prevent memory growth"""
        current_time = time.time()

        # Only cleanup periodically
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = current_time
        initial_count = len(self._recent_announcements)

        # Clean global announcements
        self._recent_announcements = {
            news_id: timestamp for news_id, timestamp in self._recent_announcements.items()
            if current_time - timestamp < self._global_duplicate_window
        }

        # Clean announcement hashes
        self._announcement_hashes = {
            content_hash: timestamp for content_hash, timestamp in self._announcement_hashes.items()
            if current_time - timestamp < self._hash_duplicate_window
        }

        # Clean user announcements
        for user_id in list(self._user_announcements.keys()):
            self._user_announcements[user_id].clear()

        if self._verbose and initial_count > 0:
            cleaned = initial_count - len(self._recent_announcements)
            print(f"🧹 BSE Dedup Tracker: Cleaned {cleaned} old entries")

    def is_duplicate_announcement(self, user_id: str, news_id: str, headline: str,
                                company_name: str, ann_dt: str) -> Tuple[bool, str]:
        """
        Check if announcement is a duplicate based on multiple criteria

        Returns:
            (is_duplicate, reason)
        """
        current_time = time.time()

        # Cleanup old entries periodically
        self._cleanup_old_entries()

        # 1. Check rapid refetch window (most important - prevents the issue you described)
        if news_id in self._recent_announcements:
            time_diff = current_time - self._recent_announcements[news_id]
            if time_diff < self._rapid_refetch_window:
                if self._verbose:
                    print(f"🚫 BSE DUPLICATE: Rapid refetch prevented for {news_id} ({time_diff:.1f}s ago)")
                return True, f"rapid_refetch_{time_diff:.0f}s"

        # 2. Check global duplicate window (prevent same announcement to anyone)
        if news_id in self._recent_announcements:
            time_diff = current_time - self._recent_announcements[news_id]
            if time_diff < self._global_duplicate_window:
                if self._verbose:
                    print(f"🌍 BSE DUPLICATE: Global duplicate prevented for {news_id} ({time_diff:.1f}s ago)")
                return True, f"global_duplicate_{time_diff:.0f}s"

        # 3. Check user-specific duplicates
        if news_id in self._user_announcements[user_id]:
            if self._verbose:
                print(f"👤 BSE DUPLICATE: User duplicate prevented for {user_id[:8]} - {news_id}")
            return True, "user_duplicate"

        # 4. Check content-based duplicates (catch similar announcements)
        content_hash = self._generate_content_hash(headline, company_name, ann_dt)
        if content_hash in self._announcement_hashes:
            time_diff = current_time - self._announcement_hashes[content_hash]
            if time_diff < self._hash_duplicate_window:
                if self._verbose:
                    print(f"📝 BSE DUPLICATE: Content hash duplicate prevented for {content_hash}")
                return True, f"content_hash_duplicate_{time_diff:.0f}s"

        # Not a duplicate
        return False, "new"

    def mark_announcement_sent(self, user_id: str, news_id: str, headline: str,
                              company_name: str, ann_dt: str):
        """Mark announcement as sent to prevent future duplicates"""
        current_time = time.time()

        # Add to global tracking
        self._recent_announcements[news_id] = current_time

        # Add to user tracking
        self._user_announcements[user_id].add(news_id)

        # Add content hash tracking
        content_hash = self._generate_content_hash(headline, company_name, ann_dt)
        self._announcement_hashes[content_hash] = current_time

        if self._verbose:
            print(f"✅ BSE TRACKED: Marked {news_id} sent to {user_id[:8]} (hash: {content_hash})")

    def get_tracking_stats(self) -> Dict:
        """Get current tracking statistics"""
        current_time = time.time()

        # Count active announcements within windows
        active_global = sum(
            1 for timestamp in self._recent_announcements.values()
            if current_time - timestamp < self._global_duplicate_window
        )

        active_hashes = sum(
            1 for timestamp in self._announcement_hashes.values()
            if current_time - timestamp < self._hash_duplicate_window
        )

        return {
            'active_global_announcements': active_global,
            'active_content_hashes': active_hashes,
            'total_users_tracking': len(self._user_announcements),
            'total_memory_entries': len(self._recent_announcements) + len(self._announcement_hashes),
            'last_cleanup': datetime.fromtimestamp(self._last_cleanup).isoformat()
        }

    def force_cleanup(self):
        """Force cleanup of old entries"""
        self._last_cleanup = 0  # Reset timer to force cleanup
        self._cleanup_old_entries()

    def clear_user_tracking(self, user_id: str):
        """Clear tracking for specific user (useful for testing)"""
        if user_id in self._user_announcements:
            del self._user_announcements[user_id]
        if self._verbose:
            print(f"🗑️ BSE TRACKING: Cleared tracking for user {user_id[:8]}")

# Global tracker instance
_tracker_instance = None

def get_bse_tracker():
    """Get the global BSE tracker instance"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = BSEAnnouncementTracker()
    return _tracker_instance

def is_bse_duplicate(user_id: str, news_id: str, headline: str, company_name: str, ann_dt: str) -> Tuple[bool, str]:
    """
    Convenience function to check if BSE announcement is duplicate

    Args:
        user_id: User identifier
        news_id: BSE news ID
        headline: Announcement headline
        company_name: Company name
        ann_dt: Announcement datetime

    Returns:
        (is_duplicate, reason)
    """
    tracker = get_bse_tracker()
    return tracker.is_duplicate_announcement(user_id, news_id, headline, company_name, ann_dt)

def mark_bse_sent(user_id: str, news_id: str, headline: str, company_name: str, ann_dt: str):
    """
    Convenience function to mark BSE announcement as sent
    """
    tracker = get_bse_tracker()
    tracker.mark_announcement_sent(user_id, news_id, headline, company_name, ann_dt)

def get_bse_tracking_stats() -> Dict:
    """Get BSE tracking statistics"""
    tracker = get_bse_tracker()
    return tracker.get_tracking_stats()

# Flask app integration
def add_bse_dedup_endpoints(app):
    """Add BSE deduplication endpoints to Flask app"""

    @app.route('/debug/bse_dedup_stats')
    def bse_dedup_stats():
        """Get BSE deduplication statistics"""
        try:
            stats = get_bse_tracking_stats()
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'stats': stats
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }, 500

    @app.route('/admin/bse_dedup_cleanup', methods=['POST'])
    def bse_dedup_cleanup():
        """Force cleanup BSE deduplication tracking"""
        try:
            tracker = get_bse_tracker()
            tracker.force_cleanup()
            return {
                'status': 'success',
                'message': 'BSE deduplication tracking cleaned up',
                'stats': tracker.get_tracking_stats()
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }, 500