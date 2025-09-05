# News Tracking Implementation

## Overview

This document explains how the news tracking functionality works to prevent duplicate notifications in the BSE Monitor system.

## Problem Statement

The system was sending all recent news articles every 30 minutes, rather than only sending new articles that had not been sent before. This resulted in users receiving duplicate notifications for the same news articles.

## Solution

The solution implements a tracking mechanism that:

1. Generates unique identifiers for each news article
2. Checks if an article has already been sent in the past 48 hours
3. Only sends notifications for new articles
4. Stores information about sent articles to prevent future duplicates

## Implementation Details

### Unique Article Identification

Each article is assigned a unique identifier based on:
- The article's URL (if available)
- The article's title (if no URL is available)
- A timestamp-based ID (if neither URL nor title is available)

The ID is generated using MD5 hashing to ensure consistency.

### Duplicate Checking

The `check_news_already_sent` function queries the database to see if an article with the same ID has been processed for the same company within the last 48 hours.

### Storing Sent Articles

After sending a notification, the `store_sent_news_article` function stores information about the article in the `processed_news_articles` table, including:
- Article ID
- Title
- URL
- Source
- Publication date
- Company name
- User ID

### Time Window

Articles are tracked for 48 hours to ensure that:
- Recently sent articles are not resent
- Very old articles that might reappear in feeds are still sent if they're new to the user

## Files Modified

- `enhanced_news_monitor.py`: Added duplicate checking and storage functionality

## Functions Added

### `check_news_already_sent(user_client, article, company_name)`
Checks if an article has already been sent for a specific company.

### `store_sent_news_article(user_client, article, company_name, user_id)`
Stores information about a sent article to prevent duplicates.

## Usage

The enhanced news monitoring now works as follows:

1. Fetch news articles for each monitored company
2. Filter articles to only include today's news
3. For each article, check if it has already been sent
4. Only process and send notifications for new articles
5. Store information about sent articles to prevent future duplicates

## Benefits

- Users only receive notifications for new articles
- Reduced notification volume and improved user experience
- More efficient use of system resources
- Better tracking of what news has been sent to each user

## Testing

The implementation includes test scripts to verify that:
- The tracking functions can be imported correctly
- New articles are correctly identified as not sent
- Article information can be stored successfully