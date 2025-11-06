#!/usr/bin/env python3
"""
Bulk Deal Monitoring System
Daily bulk deal monitoring for monitored stocks
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List
from bulk_deal_fetcher import BulkDealFetcher, format_bulk_deal_message

logger = logging.getLogger(__name__)

def send_bulk_deal_alerts(user_client, user_id: str, monitored_scrips: List[Dict],
                         telegram_recipients: List[Dict]) -> int:
    """
    Send bulk deal alerts for monitored stocks.
    Runs once daily to check for bulk deals from previous trading day.

    Returns number of messages sent.
    """
    messages_sent = 0

    if not monitored_scrips or not telegram_recipients:
        return 0

    try:
        # Initialize bulk deal fetcher
        fetcher = BulkDealFetcher()

        # Get previous trading day (bulk deals are available for previous day)
        previous_trading_day = fetcher.get_previous_trading_day()

        # Fetch all bulk deals for previous trading day
        all_bulk_deals = fetcher.fetch_all_bulk_deals(previous_trading_day)

        if not all_bulk_deals:
            logger.info(f"No bulk deals found for {previous_trading_day}")
            return 0

        # Filter deals for monitored stocks only
        monitored_deals = fetcher.filter_monitored_stocks(all_bulk_deals, monitored_scrips)

        if not monitored_deals:
            logger.info(f"No bulk deals found for monitored stocks on {previous_trading_day}")
            return 0

        # Check if we already sent bulk deal alerts for this date
        if _has_sent_bulk_alerts_today(user_client, user_id, previous_trading_day):
            logger.info(f"Bulk deal alerts already sent for {previous_trading_day}")
            return 0

        # Format message
        message = format_bulk_deal_message(monitored_deals)

        if not message:
            logger.warning("Failed to format bulk deal message")
            return 0

        # Add header with trading date
        header = f"📈 Bulk Deals Alert - {previous_trading_day.strftime('%d %b %Y')}\n"
        full_message = header + message

        # Send to all recipients
        for recipient in telegram_recipients:
            try:
                user_name = recipient.get('user_name', 'User')
                _send_telegram_message_with_user_name(recipient['chat_id'], full_message, user_name)
                messages_sent += 1
                logger.info(f"Sent bulk deal alert to {user_name}")
            except Exception as e:
                logger.error(f"Failed to send bulk deal alert: {e}")

        # Record that we sent alerts for this date
        _record_bulk_alert_sent(user_client, user_id, previous_trading_day, len(monitored_deals))

        logger.info(f"Sent {messages_sent} bulk deal alerts for {len(monitored_deals)} deals on {previous_trading_day}")

    except Exception as e:
        logger.error(f"Error in send_bulk_deal_alerts: {e}")

    return messages_sent

def _has_sent_bulk_alerts_today(user_client, user_id: str, target_date: date) -> bool:
    """Check if bulk deal alerts were already sent for the target date"""
    try:
        from database import ALERTS_TABLE

        resp = (
            user_client.table(ALERTS_TABLE)
            .select('user_id', count='exact')
            .eq('user_id', user_id)
            .eq('alert_date', target_date.isoformat())
            .eq('alert_type', 'bulk_deals')
            .execute()
        )
        return (getattr(resp, 'count', 0) or 0) > 0
    except Exception:
        return False

def _record_bulk_alert_sent(user_client, user_id: str, target_date: date, deal_count: int):
    """Record that bulk deal alerts were sent for the target date"""
    try:
        from database import ALERTS_TABLE

        user_client.table(ALERTS_TABLE).insert({
            'user_id': user_id,
            'bse_code': 'BULK_DEALS',
            'alert_date': target_date.isoformat(),
            'alert_type': f'bulk_deals_{deal_count}',
        }).execute()
    except Exception as e:
        logger.error(f"Failed to record bulk alert sent: {e}")

def _send_telegram_message_with_user_name(chat_id: str, message: str, user_name: str):
    """Send Telegram message with user name (imported from database module)"""
    try:
        from database import send_telegram_message_with_user_name
        send_telegram_message_with_user_name(chat_id, message, user_name)
    except ImportError:
        # Fallback if function not available
        import requests

        telegram_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        if not telegram_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not configured")

        url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }

        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()

def get_bulk_deal_summary(user_client, user_id: str, monitored_scrips: List[Dict],
                         days_back: int = 7) -> Dict:
    """
    Get summary of bulk deals for monitored stocks over past few days
    """
    try:
        fetcher = BulkDealFetcher()
        summary = {
            'total_deals': 0,
            'daily_breakdown': {},
            'top_stocks': {},
            'buy_sell_breakdown': {'Buy': 0, 'Sell': 0}
        }

        for days_ago in range(days_back):
            target_date = date.today() - timedelta(days=days_ago)

            # Skip weekends
            if target_date.weekday() >= 5:
                continue

            all_deals = fetcher.fetch_all_bulk_deals(target_date)
            monitored_deals = fetcher.filter_monitored_stocks(all_deals, monitored_scrips)

            if monitored_deals:
                date_str = target_date.strftime('%Y-%m-%d')
                summary['daily_breakdown'][date_str] = len(monitored_deals)
                summary['total_deals'] += len(monitored_deals)

                # Track buy/sell breakdown
                for deal in monitored_deals:
                    direction = deal['buy_sell']
                    if direction in summary['buy_sell_breakdown']:
                        summary['buy_sell_breakdown'][direction] += 1

                    # Track top stocks
                    stock_key = f"{deal['security_name']} ({deal['security_code']})"
                    if stock_key not in summary['top_stocks']:
                        summary['top_stocks'][stock_key] = 0
                    summary['top_stocks'][stock_key] += 1

        # Sort top stocks by deal count
        summary['top_stocks'] = dict(sorted(summary['top_stocks'].items(),
                                          key=lambda x: x[1], reverse=True)[:10])

        return summary

    except Exception as e:
        logger.error(f"Error getting bulk deal summary: {e}")
        return {'error': str(e)}

# Test function
def test_bulk_deal_monitor():
    """Test the bulk deal monitor"""
    print("Testing Bulk Deal Monitor")
    print("=" * 40)

    # Mock data for testing
    mock_scrips = [
        {'bse_code': '500463', 'company_name': 'Test Company Ltd'},
        {'bse_code': '500112', 'company_name': 'Another Test Corp'},
    ]

    mock_recipients = [
        {'chat_id': '123456789', 'user_name': 'Test User'}
    ]

    # Mock user client
    class MockUserClient:
        def table(self, table_name):
            return MockTable()

    class MockTable:
        def select(self, columns, count=None):
            return MockResponse(0)

        def insert(self, data):
            print(f"Mock insert: {data}")
            return MockResponse()

    class MockResponse:
        def __init__(self, count=0):
            self.count = count

        def execute(self):
            return self

    # Test bulk deal fetching
    fetcher = BulkDealFetcher()
    yesterday = date.today() - timedelta(days=1)

    print(f"Fetching bulk deals for {yesterday}")
    deals = fetcher.fetch_all_bulk_deals(yesterday)
    print(f"Found {len(deals)} total deals")

    # Test filtering
    if deals:
        monitored_deals = fetcher.filter_monitored_stocks(deals, mock_scrips)
        print(f"Filtered to {len(monitored_deals)} monitored deals")

        if monitored_deals:
            # Test message formatting
            message = format_bulk_deal_message(monitored_deals)
            print("\nFormatted message:")
            print(message[:500] + "..." if len(message) > 500 else message)

if __name__ == "__main__":
    test_bulk_deal_monitor()