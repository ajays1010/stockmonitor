#!/usr/bin/env python3
"""
Script to get and test a valid Fyers token
"""

import logging
import os
import requests
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_with_valid_token():
    """Test getting live Fyers data with a valid token"""
    try:
        # Import our implementation
        # # from fyers_... (removed)
        
        logger.info("Testing with current environment configuration...")
        
        # Check if we have a valid token
        access_token = os.environ.get('FYERS_ACCESS_TOKEN')
        app_id = os.environ.get('FYERS_APP_ID')
        
        if not access_token or access_token == 'your-fyers-access-token-here':
            logger.warning("No valid FYERS_ACCESS_TOKEN found in environment")
            logger.info("To get a valid token:")
            logger.info("Option 1 - Use the new two-phase setup:")
            logger.info("  Run: python fyers_token_setup.py")
            logger.info("Option 2 - Traditional OAuth flow:")
            logger.info("  1. Visit http://localhost:5000/fyers/login in your browser")
            logger.info("  2. Complete the Fyers OAuth flow")
            logger.info("  3. Check http://localhost:5000/fyers/status to verify authentication")
            return False
        
        logger.info(f"Found FYERS_APP_ID: {app_id[:10]}...")
        logger.info(f"Found FYERS_ACCESS_TOKEN: {access_token[:30]}...")
        
        # Test the adapter
        logger.info("Testing Fyers adapter...")
        adapter = None  # get_fyers_adapter() removed
        
        if adapter and adapter.is_connected():
            logger.info("✅ Fyers adapter is connected")
            
            # Test symbols
            symbols = ["RELIANCE", "HDFCBANK"]
            
            for symbol in symbols:
                logger.info(f"Testing live data for {symbol}...")
                
                # Get price data
                price_data = adapter.get_smart_price_data(symbol)
                
                if price_data and price_data.get('data_available'):
                    logger.info("✅ SUCCESS: Got live price data")
                    logger.info(f"Symbol: {price_data.get('symbol')}")
                    logger.info(f"Price: {price_data.get('formatted_price')}")
                    logger.info(f"Change: {price_data.get('formatted_change')}")
                    logger.info(f"Volume: {price_data.get('volume_formatted')}")
                    logger.info(f"Market Status: {price_data.get('market_status')}")
                    logger.info(f"Data Source: {price_data.get('data_source')}")
                    
                    if 'placeholder' not in price_data.get('data_source', ''):
                        logger.info("✅ CONFIRMED: Getting LIVE data from Fyers API")
                    else:
                        logger.warning("⚠️  WARNING: Getting placeholder data, not live data")
                    
                    # Test the formatted message
                    message = f"""
📊 *{price_data.get('symbol')}* Price Alert 📊
Current Price: *{price_data.get('formatted_price')}*
Change: *{price_data.get('formatted_change')}*
Volume: {price_data.get('volume_formatted')}

Day Range: ₹{price_data.get('low'):.2f} - ₹{price_data.get('high'):.2f}
Market Status: {price_data.get('market_status').upper()}
"""
                    logger.info("Sample message:")
                    print(message)
                    return True
                else:
                    error_msg = price_data.get('error', 'Unknown error') if price_data else 'No data returned'
                    logger.error(f"❌ ERROR: Failed to get price data for {symbol}: {error_msg}")
        else:
            logger.error("❌ Fyers adapter is not connected")
            logger.info("To fix this:")
            logger.info("1. Visit http://localhost:5000/fyers/login in your browser")
            logger.info("2. Complete the Fyers OAuth flow")
            return False
            
        return True
            
    except Exception as e:
        logger.error(f"❌ ERROR: Test failed with exception: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    logger.info("Starting Fyers token validation test...")
    result = test_with_valid_token()
    
    if result:
        logger.info("✅ TEST PASSED: Fyers token is valid and working")
    else:
        logger.error("❌ TEST FAILED: Fyers token is not valid or not working")