#!/usr/bin/env python3
"""
Simulate the Fyers authentication flow to test the session context fix
"""

import os
import sys
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def simulate_auth_flow():
    """Simulate the Fyers authentication flow"""
    try:
        # Import the Fyers components
        # # from fyers_... (removed)
        
        logger.info("Simulating Fyers authentication flow...")
        
        # Get session manager
        session_manager = None  # get_session_manager() removed
        logger.info(f"Session manager: {session_manager}")
        
        # Create a mock session object to simulate Flask session
        class MockSession(dict):
            def __init__(self):
                super().__init__()
                self.permanent = True
                self.modified = False
        
        # Create mock session
        mock_session = MockSession()
        
        # Simulate storing a token in the session (as would happen after OAuth)
        test_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIl0sImF0X2hhc2giOiJnQUFBQUFCb3R4MU5JeFM1MEVFMG5GWjMtUXJlRkhLeGhBT3YtSUJyNFpWQUZvLVlacGd3Rko3RnF6cjFGQ1lZQUtxNTVDLXpZWXJKb2F5WjEwNzhWUVphdmxVcWRjOE5BTmdlcTd6WFJsX2dDZk5ISjRrWTZ0Zz0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJlYTQ3NTFmMDdjODg5NGViZGUzZGYzN2Y4Y2Q0OGE4MDI2NGJlZjQ4OGM5NGYxMDBiZjA2Y2NjZiIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiRkFEMDEyMDkiLCJhcHBUeXBlIjoxMDAsImV4cCI6MTc1Njg1OTQwMCwiaWF0IjoxNzU2ODMxMDUzLCJpc3MiOiJhcGkuZnllcnMuaW4iLCJuYmYiOjE3NTY4MzEwNTMsInN1YiI6ImFjY2Vzc190b2tlbiJ9.yFVsM2spFPkQpvWGPFyJwjyOHQeO5ym1CsA0P6b-kmo"
        
        # Store token in session (as would happen in the OAuth callback)
        if session_manager.store_token(mock_session, test_token):
            logger.info("✅ Token stored successfully in mock session")
            
            # Verify token can be retrieved
            retrieved_token = session_manager.get_stored_token(mock_session)
            if retrieved_token == test_token:
                logger.info("✅ Token retrieved successfully from mock session")
                
                # Test that global cache was also updated
                # # from fyers_... (removed)
                cached_token_data = {} # _fyers_token_cache removed.get('fyers_token_data')
                if cached_token_data and cached_token_data.get('access_token') == test_token:
                    logger.info("✅ Token also stored in global cache")
                    return True
                else:
                    logger.warning("⚠️  Token not found in global cache")
                    return False
            else:
                logger.error("❌ Retrieved token doesn't match stored token")
                return False
        else:
            logger.error("❌ Failed to store token in mock session")
            return False
            
    except Exception as e:
        logger.error(f"❌ ERROR: Simulation failed with exception: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    logger.info("Starting Fyers authentication flow simulation...")
    result = simulate_auth_flow()
    
    if result:
        logger.info("✅ SIMULATION PASSED: Fyers authentication flow is working")
        sys.exit(0)
    else:
        logger.error("❌ SIMULATION FAILED: Fyers authentication flow is not working")
        sys.exit(1)