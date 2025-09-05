#!/usr/bin/env python3
"""
Price Alert API - Smart Price and Volume Data
Implements API endpoints for price alerts (Fyers API removed)
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

from flask import Blueprint, request, jsonify, session
from functools import wraps

# Fyers adapter imports removed - using alternative data source
# # from fyers_... (removed)
# # from fyers_... (removed)

def get_smart_price_message(*args, **kwargs):
    return {"error": "Market data service not available"}

def get_fyers_adapter_from_session():
    return None

def is_fyers_authenticated():
    return False

# Set up logging
logger = logging.getLogger(__name__)

# Create Blueprint
price_alert_api = Blueprint('price_alert_api', __name__, url_prefix='/api/price')

# --- Helper Functions ---

def api_login_required(f):
    """Decorator to ensure user is logged in for API routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = session.get('user_id')
        if not user_id:
            return jsonify({
                'success': False,
                'error': 'Authentication required',
                'message': 'You must be logged in to use this API'
            }), 401
        return f(*args, **kwargs)
    return decorated_function

def fyers_required(f):
    """Decorator to ensure Fyers API is connected"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_fyers_authenticated(session):
            return jsonify({
                'success': False,
                'error': 'Fyers authentication required',
                'message': 'You need to connect your Fyers account first',
                'redirect': '/fyers/login'
            }), 403
        return f(*args, **kwargs)
    return decorated_function

# --- API Routes ---

@price_alert_api.route('/status')
@api_login_required
def api_status():
    """Get price alert API status"""
    
    # Check if Fyers is connected
    fyers_connected = is_fyers_authenticated(session)
    
    status = {
        'success': True,
        'api_status': 'online',
        'fyers_connected': fyers_connected,
        'timestamp': datetime.now().isoformat(),
        'features': [
            'smart_price_data',
            'price_alerts',
            'volume_alerts'
        ],
        'message': 'Price Alert API is available' if fyers_connected else 'Fyers connection required'
    }
    
    if not fyers_connected:
        status['connect_url'] = '/fyers/login'
    
    return jsonify(status)

@price_alert_api.route('/smart-price/<symbol>')
@api_login_required
@fyers_required
def smart_price(symbol):
    """Get smart price data for a symbol"""
    try:
        # Normalize symbol
        symbol = symbol.upper()
        
        # Try to get token from global manager first (new preferred approach)
        session_token = None
        try:
            # from fyers_... (removed)
            session_token = None  # get_fyers_token() removed
            if session_token:
                logger.info("Using token from global token manager")
        except Exception as global_manager_error:
            logger.warning(f"Global token manager not available: {global_manager_error}")
        
        # Fallback to session token
        if not session_token:
            # from fyers_... (removed)
            session_mgr = None  # get_session_manager() removed
            session_token = session_mgr.get_stored_token(session)
            logger.info("Using token from Flask session")
        
        # Get smart price data using the Fyers adapter with explicit token
        price_data = get_smart_price_message(symbol, session_token=session_token)
        
        if not price_data or not price_data.get('data_available'):
            return jsonify({
                'success': False,
                'error': price_data.get('error', 'Failed to get price data'),
                'symbol': symbol
            }), 400
        
        # Return successful response
        return jsonify({
            'success': True,
            'symbol': symbol,
            'data': price_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in smart-price API for {symbol}: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'symbol': symbol
        }), 500

@price_alert_api.route('/batch-prices', methods=['POST'])
@api_login_required
@fyers_required
def batch_prices():
    """Get price data for multiple symbols in one request"""
    try:
        # Get symbols from request body
        data = request.get_json()
        if not data or 'symbols' not in data:
            return jsonify({
                'success': False,
                'error': 'No symbols provided'
            }), 400
        
        symbols = data['symbols']
        if not symbols or not isinstance(symbols, list):
            return jsonify({
                'success': False,
                'error': 'Invalid symbols format'
            }), 400
        
        # Normalize symbols
        symbols = [s.upper() for s in symbols]
        
        # Limit number of symbols
        max_symbols = 10
        if len(symbols) > max_symbols:
            return jsonify({
                'success': False,
                'error': f'Too many symbols. Maximum allowed: {max_symbols}'
            }), 400
        
        # Try to get token from global manager first (new preferred approach)
        session_token = None
        try:
            # from fyers_... (removed)
            session_token = None  # get_fyers_token() removed
            if session_token:
                logger.info("Using token from global token manager")
        except Exception as global_manager_error:
            logger.warning(f"Global token manager not available: {global_manager_error}")
        
        # Fallback to session token
        if not session_token:
            # from fyers_... (removed)
            session_mgr = None  # get_session_manager() removed
            session_token = session_mgr.get_stored_token(session)
            logger.info("Using token from Flask session")
        
        # Get price data for each symbol
        results = {}
        for symbol in symbols:
            price_data = get_smart_price_message(symbol, session_token=session_token)
            results[symbol] = price_data
        
        # Return all results
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in batch-prices API: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@price_alert_api.route('/format-message/<symbol>')
@api_login_required
@fyers_required
def format_message(symbol):
    """Get a formatted price alert message for a symbol"""
    try:
        # Normalize symbol
        symbol = symbol.upper()
        
        # Try to get token from global manager first (new preferred approach)
        session_token = None
        try:
            # from fyers_... (removed)
            session_token = None  # get_fyers_token() removed
            if session_token:
                logger.info("Using token from global token manager")
        except Exception as global_manager_error:
            logger.warning(f"Global token manager not available: {global_manager_error}")
        
        # Fallback to session token
        if not session_token:
            # from fyers_... (removed)
            session_mgr = None  # get_session_manager() removed
            session_token = session_mgr.get_stored_token(session)
            logger.info("Using token from Flask session")
        
        # Get smart price data with explicit token
        price_data = get_smart_price_message(symbol, session_token=session_token)
        
        if not price_data or not price_data.get('data_available'):
            return jsonify({
                'success': False,
                'error': price_data.get('error', 'Failed to get price data'),
                'symbol': symbol
            }), 400
        
        # Format the message for display or sending
        emoji_prefix = "🔺" if price_data.get('price_change', 0) > 0 else "🔻" if price_data.get('price_change', 0) < 0 else "➡️"
        
        # Create formatted message
        formatted_message = f"""{emoji_prefix} *{symbol}* Price Alert {emoji_prefix}
Current Price: *{price_data.get('formatted_price')}*
Change: *{price_data.get('formatted_change')}*
Volume: {price_data.get('volume_formatted')}

Day Range: ₹{price_data.get('low'):.2f} - ₹{price_data.get('high'):.2f}
Market Status: {price_data.get('market_status', 'unknown').upper()}
        """
        
        # Return message along with raw data
        return jsonify({
            'success': True,
            'symbol': symbol,
            'formatted_message': formatted_message,
            'data': price_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error formatting message for {symbol}: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'symbol': symbol
        }), 500

# Export the blueprint for registration with app
def register_price_alert_api(app):
    app.register_blueprint(price_alert_api)
    logger.info("Price Alert API registered successfully")