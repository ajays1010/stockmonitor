#!/usr/bin/env python3
"""
Bulk Deal Data Fetcher for BSE and NSE
Fetches bulk deal data and checks for monitored stocks
"""

import requests
import pandas as pd
import re
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
import json
import time

logger = logging.getLogger(__name__)

class BulkDealFetcher:
    """Fetches bulk deal data from BSE and NSE exchanges"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # BSE specific headers
        self.bse_headers = {
            'Referer': 'https://www.bseindia.com/',
            'Origin': 'https://www.bseindia.com',
        }

    def get_previous_trading_day(self) -> date:
        """Get previous trading day (skip weekends)"""
        today = date.today()
        previous_day = today - timedelta(days=1)

        # Skip weekends
        while previous_day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            previous_day -= timedelta(days=1)

        return previous_day

    def fetch_bse_bulk_deals(self, target_date: date = None) -> List[Dict]:
        """
        Fetch bulk deals from BSE for the specified date

        Returns list of bulk deal data with fields:
        - security_code: BSE code
        - security_name: Company name
        - client_name: Client name
        - buy_sell: 'Buy' or 'Sell'
        - quantity: Number of shares
        - weighted_avg_price: Price per share
        - deal_date: Date of deal
        - exchange: 'BSE'
        """
        if target_date is None:
            target_date = self.get_previous_trading_day()

        try:
            # BSE bulk deals API endpoint
            url = "https://api.bseindia.com/BseIndiaAPI/api/BulkDeals/w"

            # Format date for BSE API (DD-MM-YYYY)
            date_str = target_date.strftime('%d-%m-%Y')

            params = {
                'ddlDate': date_str,
                'scripcode': '',  # Empty for all stocks
                'segment': 'E'   # Equity segment
            }

            headers = {**self.session.headers, **self.bse_headers}

            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not data or 'Data' not in data:
                logger.warning(f"No bulk deal data found for BSE on {date_str}")
                return []

            bulk_deals = []

            for item in data.get('Data', []):
                try:
                    # Extract data from BSE API response
                    deal = {
                        'security_code': item.get('Scrip_Code', ''),
                        'security_name': item.get('Scrip_Name', '').strip(),
                        'client_name': item.get('ClientName', '').strip(),
                        'buy_sell': item.get('BuySell', '').strip().title(),
                        'quantity': int(item.get('Quantity', 0).replace(',', '') if item.get('Quantity') else 0),
                        'weighted_avg_price': float(item.get('Weighted_Avg_Price', 0).replace(',', '') if item.get('Weighted_Avg_Price') else 0.0),
                        'deal_date': target_date.isoformat(),
                        'exchange': 'BSE'
                    }

                    # Only include deals with valid data
                    if deal['security_code'] and deal['quantity'] > 0:
                        bulk_deals.append(deal)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing BSE bulk deal item: {e}")
                    continue

            logger.info(f"Fetched {len(bulk_deals)} bulk deals from BSE for {date_str}")
            return bulk_deals

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch BSE bulk deals: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse BSE bulk deals response: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching BSE bulk deals: {e}")
            return []

    def fetch_nse_bulk_deals(self, target_date: date = None) -> List[Dict]:
        """
        Fetch bulk deals from NSE for the specified date

        NSE provides bulk deals in CSV format through their API
        """
        if target_date is None:
            target_date = self.get_previous_trading_day()

        try:
            # NSE bulk deals CSV download URL
            # Format date for NSE (DD-MM-YYYY)
            date_str = target_date.strftime('%d-%m-%Y')

            # NSE bulk deals endpoint
            url = f"https://www.nseindia.com/api/reports/bulk-deals"

            # NSE API requires specific headers
            nse_headers = {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.nseindia.com/report-detail/display-bulk-and-block-deals',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
            }

            # Try to fetch bulk deals data
            response = self.session.get(url, headers=nse_headers, timeout=30)

            if response.status_code == 200:
                try:
                    data = response.json()

                    if not data or 'data' not in data:
                        logger.warning(f"No bulk deal data found for NSE on {date_str}")
                        return []

                    bulk_deals = []

                    for item in data.get('data', []):
                        try:
                            # Extract data from NSE API response
                            deal = {
                                'security_code': item.get('symbol', ''),
                                'security_name': item.get('companyName', '').strip(),
                                'client_name': item.get('clientName', '').strip(),
                                'buy_sell': item.get('buySell', '').strip().title(),
                                'quantity': int(item.get('quantity', 0).replace(',', '') if item.get('quantity') else 0),
                                'weighted_avg_price': float(item.get('price', 0).replace(',', '') if item.get('price') else 0.0),
                                'deal_date': target_date.isoformat(),
                                'exchange': 'NSE'
                            }

                            # Only include deals with valid data
                            if deal['security_code'] and deal['quantity'] > 0:
                                bulk_deals.append(deal)

                        except (ValueError, KeyError) as e:
                            logger.warning(f"Error parsing NSE bulk deal item: {e}")
                            continue

                    logger.info(f"Fetched {len(bulk_deals)} bulk deals from NSE for {date_str}")
                    return bulk_deals

                except json.JSONDecodeError:
                    logger.warning("NSE API response is not JSON, trying alternative approach")

            # Alternative: Try to fetch CSV data if API fails
            return self._fetch_nse_bulk_deals_csv(target_date)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch NSE bulk deals: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching NSE bulk deals: {e}")
            return []

    def _fetch_nse_bulk_deals_csv(self, target_date: date) -> List[Dict]:
        """Alternative method to fetch NSE bulk deals as CSV"""
        try:
            # NSE bulk deals archive URL (may need to be updated)
            date_str = target_date.strftime('%d%m%Y')  # NSE sometimes uses DDMMYYYY format

            # This is a placeholder - NSE's bulk deal CSV structure may vary
            csv_url = f"https://archives.nseindia.com/content/equities/bulk_deals_{date_str}.csv"

            response = self.session.get(csv_url, timeout=30)

            if response.status_code == 200:
                # Parse CSV data
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))

                bulk_deals = []
                for _, row in df.iterrows():
                    try:
                        deal = {
                            'security_code': str(row.get('Symbol', '')),
                            'security_name': str(row.get('Security Name', '')).strip(),
                            'client_name': str(row.get('Client Name', '')).strip(),
                            'buy_sell': str(row.get('Buy/Sell', '')).strip().title(),
                            'quantity': int(row.get('Quantity', 0)),
                            'weighted_avg_price': float(row.get('Price', 0)),
                            'deal_date': target_date.isoformat(),
                            'exchange': 'NSE'
                        }

                        if deal['security_code'] and deal['quantity'] > 0:
                            bulk_deals.append(deal)

                    except (ValueError, KeyError) as e:
                        continue

                logger.info(f"Fetched {len(bulk_deals)} bulk deals from NSE CSV for {target_date}")
                return bulk_deals

        except Exception as e:
            logger.warning(f"Failed to fetch NSE bulk deals CSV: {e}")

        return []

    def fetch_all_bulk_deals(self, target_date: date = None) -> List[Dict]:
        """Fetch bulk deals from both BSE and NSE"""
        all_deals = []

        # Fetch from BSE
        bse_deals = self.fetch_bse_bulk_deals(target_date)
        all_deals.extend(bse_deals)

        # Small delay to avoid overwhelming servers
        time.sleep(1)

        # Fetch from NSE
        nse_deals = self.fetch_nse_bulk_deals(target_date)
        all_deals.extend(nse_deals)

        # Sort by exchange and then by quantity (largest first)
        all_deals.sort(key=lambda x: (x['exchange'], -x['quantity']))

        logger.info(f"Total bulk deals fetched: {len(all_deals)} (BSE: {len(bse_deals)}, NSE: {len(nse_deals)})")
        return all_deals

    def filter_monitored_stocks(self, bulk_deals: List[Dict], monitored_scrips: List[Dict]) -> List[Dict]:
        """Filter bulk deals to include only monitored stocks"""

        # Create set of monitored BSE codes for quick lookup
        monitored_codes = set()
        monitored_names = set()

        for scrip in monitored_scrips:
            try:
                bse_code = str(scrip['bse_code'])
                monitored_codes.add(bse_code)

                # Also add company name for matching (case-insensitive)
                company_name = scrip.get('company_name', '').strip().lower()
                if company_name:
                    monitored_names.add(company_name)

            except (KeyError, ValueError):
                continue

        filtered_deals = []

        for deal in bulk_deals:
            deal_code = str(deal.get('security_code', ''))
            deal_name = deal.get('security_name', '').strip().lower()

            # Match by BSE code (exact match)
            if deal_code in monitored_codes:
                filtered_deals.append(deal)
                continue

            # Match by company name (case-insensitive partial match)
            for monitored_name in monitored_names:
                if monitored_name in deal_name or deal_name in monitored_name:
                    filtered_deals.append(deal)
                    break

        logger.info(f"Filtered to {len(filtered_deals)} deals for monitored stocks")
        return filtered_deals

def format_bulk_deal_message(deals: List[Dict]) -> str:
    """Format bulk deals into a readable message for Telegram"""

    if not deals:
        return ""

    # Group deals by exchange and sort by quantity
    bse_deals = [d for d in deals if d['exchange'] == 'BSE']
    nse_deals = [d for d in deals if d['exchange'] == 'NSE']

    lines = ["📊 Bulk Deals Report"]
    lines.append(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # BSE Deals
    if bse_deals:
        lines.append("🏛️ BSE Bulk Deals:")
        bse_deals.sort(key=lambda x: -x['quantity'])  # Sort by quantity descending

        for deal in bse_deals:
            direction_emoji = "🔼" if deal['buy_sell'] == 'Buy' else "🔻"
            lines.append(f"• {deal['security_name']} ({deal['security_code']})")
            lines.append(f"  {direction_emoji} {deal['buy_sell']}: {deal['quantity']:,} shares @ ₹{deal['weighted_avg_price']:.2f}")
            lines.append(f"  Client: {deal['client_name']}")
            lines.append("")

    # NSE Deals
    if nse_deals:
        lines.append("📈 NSE Bulk Deals:")
        nse_deals.sort(key=lambda x: -x['quantity'])  # Sort by quantity descending

        for deal in nse_deals:
            direction_emoji = "🔼" if deal['buy_sell'] == 'Buy' else "🔻"
            lines.append(f"• {deal['security_name']} ({deal['security_code']})")
            lines.append(f"  {direction_emoji} {deal['buy_sell']}: {deal['quantity']:,} shares @ ₹{deal['weighted_avg_price']:.2f}")
            lines.append(f"  Client: {deal['client_name']}")
            lines.append("")

    # Add summary
    total_bse = len(bse_deals)
    total_nse = len(nse_deals)
    lines.append(f"📋 Summary: {total_bse} BSE deals, {total_nse} NSE deals")

    return "\n".join(lines).strip()

# Test function
def test_bulk_deal_fetcher():
    """Test the bulk deal fetcher"""
    fetcher = BulkDealFetcher()

    # Test with yesterday's date
    yesterday = date.today() - timedelta(days=1)

    print(f"Testing bulk deal fetcher for {yesterday}")

    # Fetch BSE deals
    bse_deals = fetcher.fetch_bse_bulk_deals(yesterday)
    print(f"BSE deals fetched: {len(bse_deals)}")

    # Fetch NSE deals
    nse_deals = fetcher.fetch_nse_bulk_deals(yesterday)
    print(f"NSE deals fetched: {len(nse_deals)}")

    # Fetch all deals
    all_deals = fetcher.fetch_all_bulk_deals(yesterday)
    print(f"Total deals fetched: {len(all_deals)}")

    # Show sample deals
    if all_deals:
        print("\nSample deals:")
        for deal in all_deals[:3]:
            print(f"  {deal['exchange']}: {deal['security_name']} ({deal['security_code']}) - {deal['buy_sell']} {deal['quantity']:,} @ ₹{deal['weighted_avg_price']:.2f}")

if __name__ == "__main__":
    test_bulk_deal_fetcher()