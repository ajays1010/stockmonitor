# Smart Price Implementation with Fyers API

This document outlines the implementation of the Smart Price Alert feature using the Fyers API. This feature enables the BSE Monitor system to retrieve detailed price and volume data for use in price alerts and notifications.

## Overview

The Smart Price Alert feature uses the Fyers API to fetch real-time market data for BSE stocks. It provides:

- Real-time price information
- Volume data with smart formatting
- Price change calculations (absolute and percentage)
- Market status indicators
- Day range information (high/low)

## Implementation Components

### 1. Core Adapter Methods

The main implementation is in `fyers_production_adapter.py` which includes:

- `get_smart_price_data()` - Fetches and formats comprehensive price data
- `_format_volume()` - Helper function to format volume numbers with appropriate scale (K, M)
- `get_smart_price_message()` - Convenient wrapper function for easy access

### 2. API Endpoints

The API endpoints are defined in `price_alert_api.py`:

- `/api/price/status` - Check API and Fyers connection status
- `/api/price/smart-price/{symbol}` - Get smart price data for a single symbol
- `/api/price/batch-prices` - Get price data for multiple symbols in one request
- `/api/price/format-message/{symbol}` - Get a formatted price alert message ready for display/notification

### 3. User Interface

A web interface is provided at `/price-alerts` with the template `price_alerts.html`, which offers:

- Real-time price monitoring
- Price change visualization
- Watchlist management
- Automatic refresh of data
- Mobile-friendly responsive design

## Usage

### Getting Smart Price Data in Python

```python
from fyers_production_adapter import get_smart_price_message

# Get price data for a symbol
price_data = get_smart_price_message('RELIANCE')

if price_data and price_data.get('data_available'):
    # Access the data
    current_price = price_data.get('price')
    formatted_price = price_data.get('formatted_price')  # ₹1234.56
    percent_change = price_data.get('percent_change')
    volume = price_data.get('volume') 
    volume_formatted = price_data.get('volume_formatted')  # 1.2M
```

### API Request Examples

```
GET /api/price/smart-price/RELIANCE
GET /api/price/format-message/HDFCBANK
POST /api/price/batch-prices  (body: {"symbols": ["RELIANCE", "TCS", "INFY"]})
```

## Integration with BSE Monitor

This feature integrates with the existing BSE Monitor system to:

1. Provide real-time data for the price alert system
2. Enhance Telegram notifications with detailed price information
3. Support the dashboard with live market data
4. Offer a dedicated price alerts page for monitoring

## Benefits

- **Comprehensive Data**: More detailed price information than available in BSE API
- **Better Formatting**: Smart volume formatting and price change presentation
- **Real-Time Updates**: Live market data when market is open
- **Consistent Experience**: Works with existing Fyers authentication system
- **Efficient API Usage**: Batch requests reduce API load for multiple symbols

## Future Enhancements

- Historical price charts integration
- Price alert threshold configuration
- Personalized price alert messages
- Technical indicator calculation based on price data