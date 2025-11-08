#!/usr/bin/env python3
"""
Persistent BSE Announcement Deduplication Tracker
Combines in-memory tracking with persistent file storage
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, Tuple
from collections import defaultdict
import tempfile

class PersistentBSETracker:
    """
    Persistent BSE announcement tracker that survives worker restarts
    Combines fast in-memory tracking with file-based persistence
    """

    def __init__(self, storage_file: str = None):
        # Storage file for persistence
        if storage_file is None:
            # Use temp directory for cross-platform compatibility
            temp_dir = tempfile.gettempdir()
            storage_file = os.path.join(temp_dir, 'bse_announcements_tracker.json')

        self.storage_file = storage_file

        # In-memory tracking (fast access)
        self._recent_announcements: Dict[str, float] = {}
        self._user_announcements: Dict[str, Set[str]] = defaultdict(set)
        self._announcement_hashes: Dict[str, float] = {}

        # Persistence settings
        self._last_save = time.time()
        self._save_interval = 60  # Save every 60 seconds
        self._cleanup_interval = 300  # Cleanup every 5 minutes

        # Time windows (same as original)
        self._global_duplicate_window = 7200  # 2 hours
        self._user_duplicate_window = 86400   # 24 hours
        self._hash_duplicate_window = 3600    # 1 hour
        self._rapid_refetch_window = 420      # 7 minutes (covers 5-min cron + buffer)

        # Thread safety
        self._lock = threading.RLock()

        # Enable verbose logging
        self._verbose = os.environ.get('BSE_VERBOSE', '0') == '1'

        # Load existing data
        self._load_from_disk()

        # Start background persistence thread
        self._start_persistence_thread()

        if self._verbose:
            print(f"PERSISTENT BSE TRACKER: Initialized with storage at {storage_file}")
            self._log_tracking_stats()

    def _load_from_disk(self):
        """Load tracking data from persistent storage"""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    data = json.load(f)

                # Load data with time validation
                current_time = time.time()

                # Load recent announcements (within time window)
                for news_id, timestamp in data.get('recent_announcements', {}).items():
                    if current_time - timestamp < self._global_duplicate_window:
                        self._recent_announcements[news_id] = timestamp

                # Load user announcements (always load - they're cleared by time-based logic)
                for user_id, news_ids in data.get('user_announcements', {}).items():
                    self._user_announcements[user_id] = set(news_ids)

                # Load announcement hashes (within time window)
                for content_hash, timestamp in data.get('announcement_hashes', {}).items():
                    if current_time - timestamp < self._hash_duplicate_window:
                        self._announcement_hashes[content_hash] = timestamp

                if self._verbose:
                    print(f"PERSISTENT BSE TRACKER: Loaded {len(self._recent_announcements)} announcements from disk")

        except Exception as e:
            if self._verbose:
                print(f"PERSISTENT BSE TRACKER: Failed to load from disk: {e}")
            # Start fresh if loading fails
            self._recent_announcements = {}
            self._user_announcements = defaultdict(set)
            self._announcement_hashes = {}

    def _save_to_disk(self, force: bool = False):
        """Save tracking data to persistent storage"""
        current_time = time.time()

        # Only save at intervals or if forced
        if not force and current_time - self._last_save < self._save_interval:
            return

        try:
            with self._lock:
                data = {
                    'recent_announcements': self._recent_announcements,
                    'user_announcements': {uid: list(news_ids) for uid, news_ids in self._user_announcements.items()},
                    'announcement_hashes': self._announcement_hashes,
                    'last_updated': current_time,
                    'version': '1.0'
                }

                # Write to temporary file first, then move (atomic operation)
                temp_file = self.storage_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)

                # Atomic move
                if os.name == 'nt':  # Windows
                    if os.path.exists(self.storage_file):
                        os.remove(self.storage_file)
                os.rename(temp_file, self.storage_file)

                self._last_save = current_time

                if self._verbose:
                    print(f"PERSISTENT BSE TRACKER: Saved {len(self._recent_announcements)} announcements to disk")

        except Exception as e:
            if self._verbose:
                print(f"PERSISTENT BSE TRACKER: Failed to save to disk: {e}")

    def _start_persistence_thread(self):
        """Start background thread for periodic saves and cleanup"""
        def persistence_worker():
            while True:
                try:
                    time.sleep(30)  # Check every 30 seconds

                    with self._lock:
                        # Save to disk
                        self._save_to_disk()

                        # Cleanup old entries
                        self._cleanup_old_entries()

                except Exception as e:
                    if self._verbose:
                        print(f"PERSISTENT BSE TRACKER: Background worker error: {e}")
                    time.sleep(60)  # Wait longer on error

        thread = threading.Thread(target=persistence_worker, daemon=True)
        thread.start()

        if self._verbose:
            print("PERSISTENT BSE TRACKER: Background persistence thread started")

    def _cleanup_old_entries(self):
        """Clean up old entries to prevent memory growth"""
        current_time = time.time()

        # Clean global announcements
        old_count = len(self._recent_announcements)
        self._recent_announcements = {
            news_id: timestamp for news_id, timestamp in self._recent_announcements.items()
            if current_time - timestamp < self._global_duplicate_window
        }

        # Clean announcement hashes
        self._announcement_hashes = {
            content_hash: timestamp for content_hash, timestamp in self._announcement_hashes.items()
            if current_time - timestamp < self._hash_duplicate_window
        }

        cleaned = old_count - len(self._recent_announcements)
        if self._verbose and cleaned > 0:
            print(f"PERSISTENT BSE TRACKER: Cleaned {cleaned} old announcements")

    def _generate_content_hash(self, headline: str, company_name: str, ann_dt: str) -> str:
        """Generate a content-based hash for announcements"""
        import hashlib
        content = f"{headline}|{company_name}|{ann_dt}"
        content = content.lower().strip()
        content = ' '.join(content.split())
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def is_duplicate_announcement(self, user_id: str, news_id: str, headline: str,
                                company_name: str, ann_dt: str) -> Tuple[bool, str]:
        """
        Check if announcement is a duplicate with persistent tracking

        Returns:
            (is_duplicate, reason)
        """
        current_time = time.time()

        with self._lock:
            # 1. Check rapid refetch window
            if news_id in self._recent_announcements:
                time_diff = current_time - self._recent_announcements[news_id]
                if time_diff < self._rapid_refetch_window:
                    if self._verbose:
                        remaining_time = self._rapid_refetch_window - time_diff
                        print(f"🚫 PERSISTENT BSE DUPLICATE: Rapid refetch prevented for {news_id}")
                        print(f"   Time since last: {time_diff:.1f}s, Window: {self._rapid_refetch_window}s, Remaining: {remaining_time:.1f}s")
                    return True, f"rapid_refetch_{time_diff:.0f}s"

            # 2. Check global duplicate window
            if news_id in self._recent_announcements:
                time_diff = current_time - self._recent_announcements[news_id]
                if time_diff < self._global_duplicate_window:
                    if self._verbose:
                        print(f"PERSISTENT BSE DUPLICATE: Global duplicate prevented for {news_id} ({time_diff:.1f}s ago)")
                    return True, f"global_duplicate_{time_diff:.0f}s"

            # 3. Check user-specific duplicates
            if news_id in self._user_announcements[user_id]:
                if self._verbose:
                    print(f"PERSISTENT BSE DUPLICATE: User duplicate prevented for {user_id[:8]} - {news_id}")
                return True, "user_duplicate"

            # 4. Check content-based duplicates
            content_hash = self._generate_content_hash(headline, company_name, ann_dt)
            if content_hash in self._announcement_hashes:
                time_diff = current_time - self._announcement_hashes[content_hash]
                if time_diff < self._hash_duplicate_window:
                    if self._verbose:
                        print(f"PERSISTENT BSE DUPLICATE: Content hash duplicate prevented for {content_hash}")
                    return True, f"content_hash_duplicate_{time_diff:.0f}s"

            # Not a duplicate
            return False, "new"

    def mark_announcement_sent(self, user_id: str, news_id: str, headline: str,
                              company_name: str, ann_dt: str):
        """Mark announcement as sent with persistence"""
        current_time = time.time()

        with self._lock:
            # Add to global tracking
            self._recent_announcements[news_id] = current_time

            # Add to user tracking
            self._user_announcements[user_id].add(news_id)

            # Add content hash tracking
            content_hash = self._generate_content_hash(headline, company_name, ann_dt)
            self._announcement_hashes[content_hash] = current_time

            # Save immediately for important announcements
            self._save_to_disk(force=True)

            if self._verbose:
                print(f"PERSISTENT BSE TRACKED: Marked {news_id} sent to {user_id[:8]} (hash: {content_hash})")

    def get_tracking_stats(self) -> Dict:
        """Get current tracking statistics"""
        with self._lock:
            current_time = time.time()

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
                'storage_file': self.storage_file,
                'file_exists': os.path.exists(self.storage_file),
                'file_size_kb': round(os.path.getsize(self.storage_file) / 1024, 1) if os.path.exists(self.storage_file) else 0
            }

    def _log_tracking_stats(self):
        """Log current tracking statistics"""
        stats = self.get_tracking_stats()
        if self._verbose:
            print(f"PERSISTENT BSE TRACKER: {stats['active_global_announcements']} active, "
                  f"{stats['total_users_tracking']} users, file: {stats['file_size_kb']}KB")

    def force_save_and_cleanup(self):
        """Force immediate save and cleanup"""
        with self._lock:
            self._save_to_disk(force=True)
            self._cleanup_old_entries()
            if self._verbose:
                print("PERSISTENT BSE TRACKER: Force save and cleanup completed")

    def clear_all_tracking(self):
        """Clear all tracking data (for testing)"""
        with self._lock:
            self._recent_announcements.clear()
            self._user_announcements.clear()
            self._announcement_hashes.clear()

            # Remove storage file
            try:
                if os.path.exists(self.storage_file):
                    os.remove(self.storage_file)
            except Exception as e:
                if self._verbose:
                    print(f"PERSISTENT BSE TRACKER: Failed to remove storage file: {e}")

            if self._verbose:
                print("PERSISTENT BSE TRACKER: All tracking data cleared")

# Global persistent tracker instance
_persistent_tracker = None

def get_persistent_bse_tracker():
    """Get the global persistent BSE tracker instance"""
    global _persistent_tracker
    if _persistent_tracker is None:
        _persistent_tracker = PersistentBSETracker()
    return _persistent_tracker

def is_persistent_bse_duplicate(user_id: str, news_id: str, headline: str, company_name: str, ann_dt: str) -> Tuple[bool, str]:
    """Check if BSE announcement is duplicate using persistent tracker"""
    tracker = get_persistent_bse_tracker()
    return tracker.is_duplicate_announcement(user_id, news_id, headline, company_name, ann_dt)

def mark_persistent_bse_sent(user_id: str, news_id: str, headline: str, company_name: str, ann_dt: str):
    """Mark BSE announcement as sent using persistent tracker"""
    tracker = get_persistent_bse_tracker()
    tracker.mark_announcement_sent(user_id, news_id, headline, company_name, ann_dt)

def get_persistent_bse_stats() -> Dict:
    """Get persistent BSE tracking statistics"""
    tracker = get_persistent_bse_tracker()
    return tracker.get_tracking_stats()