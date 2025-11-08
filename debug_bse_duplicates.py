#!/usr/bin/env python3
"""
Debug script to identify BSE duplicate notification sources
"""

import os
import sys
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

def check_cron_job_frequency():
    """Check how frequently the external cron job is running"""

    # Check recent cron run logs from the database
    try:
        import database as db
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            print("❌ ERROR: Could not connect to Supabase")
            return

        # Get recent BSE announcement cron runs
        from datetime import datetime
        two_hours_ago = (datetime.now() - timedelta(hours=2)).isoformat()

        print("🔍 Checking recent BSE announcement cron runs...")
        recent_runs = sb.table('cron_run_logs')\
            .select('created_at, run_id, job, notifications_sent, user_id')\
            .eq('job', 'bse_announcements')\
            .gte('created_at', two_hours_ago)\
            .order('created_at', desc=True)\
            .execute()

        if recent_runs.data:
            print(f"📊 Found {len(recent_runs.data)} BSE cron runs in last 2 hours:")
            for run in recent_runs.data[:10]:  # Show last 10 runs
                timestamp = run['created_at']
                notifications = run['notifications_sent']
                user_id = run['user_id'][:8] if run['user_id'] else 'all'
                print(f"  • {timestamp} - User: {user_id}, Sent: {notifications}")

                # Check for suspicious patterns
                if notifications > 0:
                    print(f"    ⚠️  This run sent notifications!")
        else:
            print("ℹ️  No recent BSE cron runs found in database")

    except Exception as e:
        print(f"❌ ERROR checking cron logs: {e}")

def check_persistent_tracker_status():
    """Check the persistent BSE tracker state"""

    try:
        from bse_persistent_tracker import get_persistent_bse_tracker

        tracker = get_persistent_bse_tracker()
        stats = tracker.get_tracking_stats()

        print("\n🔧 Persistent BSE Tracker Status:")
        print(f"  • Active global announcements: {stats['active_global_announcements']}")
        print(f"  • Active content hashes: {stats['active_content_hashes']}")
        print(f"  • Total users tracking: {stats['total_users_tracking']}")
        print(f"  • Storage file: {stats['storage_file']}")
        print(f"  • File size: {stats['file_size_kb']} KB")
        print(f"  • File exists: {stats['file_exists']}")

        # Show recent announcements if any
        if hasattr(tracker, '_recent_announcements'):
            recent_items = list(tracker._recent_announcements.items())[-5:]  # Last 5
            if recent_items:
                print(f"\n📝 Recent announcements in tracker:")
                for news_id, timestamp in recent_items:
                    age_minutes = (time.time() - timestamp) / 60
                    print(f"  • {news_id} - {age_minutes:.1f} minutes ago")

    except Exception as e:
        print(f"❌ ERROR checking tracker: {e}")

def check_current_bse_announcements():
    """Check what BSE announcements are currently available"""

    try:
        import database as db

        # Get current BSE announcements for a sample scrip
        since_dt = db.ist_now() - timedelta(hours=2)
        announcements = db.fetch_bse_announcements_for_scrip('500112', since_dt)  # Reliance example

        print(f"\n📰 Current BSE announcements (sample: Reliance 500112):")
        if announcements:
            for ann in announcements[:5]:  # Show last 5
                news_id = ann['news_id']
                headline = ann.get('headline', 'No headline')[:50]
                ann_dt = ann.get('ann_dt', 'No date')
                print(f"  • {news_id} - {ann_dt} - {headline}...")
        else:
            print("  ℹ️  No recent announcements found")

    except Exception as e:
        print(f"❌ ERROR fetching announcements: {e}")

def simulate_duplicate_check():
    """Simulate the duplicate detection process"""

    print(f"\n🧪 Simulating duplicate detection...")

    try:
        from bse_persistent_tracker import is_persistent_bse_duplicate, mark_persistent_bse_sent

        # Sample announcement data
        test_user = "test-user-123"
        test_news_id = f"TEST-{int(time.time())}"
        test_headline = "Test Announcement for Debug"
        test_company = "Test Company"
        test_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # First check - should be new
        is_dup, reason = is_persistent_bse_duplicate(test_user, test_news_id, test_headline, test_company, test_date)
        print(f"  1️⃣ First check: Duplicate={is_dup}, Reason='{reason}'")

        # Mark as sent
        mark_persistent_bse_sent(test_user, test_news_id, test_headline, test_company, test_date)
        print(f"  ✅ Marked as sent")

        # Second check immediately - should be duplicate
        is_dup, reason = is_persistent_bse_duplicate(test_user, test_news_id, test_headline, test_company, test_date)
        print(f"  2️⃣ Immediate check: Duplicate={is_dup}, Reason='{reason}'")

        # Third check after 3 minutes - should still be duplicate
        time.sleep(3)  # Simulate 3 minutes wait (would be longer in reality)
        is_dup, reason = is_persistent_bse_duplicate(test_user, test_news_id, test_headline, test_company, test_date)
        print(f"  3️⃣ After 3 seconds: Duplicate={is_dup}, Reason='{reason}'")

    except Exception as e:
        print(f"❌ ERROR in simulation: {e}")

def main():
    print("🔍 BSE Duplicate Notifications Debug Tool")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().isoformat()}")

    check_cron_job_frequency()
    check_persistent_tracker_status()
    check_current_bse_announcements()
    simulate_duplicate_check()

    print("\n" + "=" * 50)
    print("🎯 Recommendations:")
    print("1. Check if external cron-job.org is running more frequently than expected")
    print("2. Verify only one cron job is configured on cron-job.org")
    print("3. Check if BSE_VERBOSE is enabled to see detailed logs")
    print("4. Consider increasing the rapid_refetch_window if needed")

if __name__ == "__main__":
    main()