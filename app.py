import os
from dotenv import load_dotenv
from functools import wraps
import sys
load_dotenv()

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify
# from supabase import create_client  # not used directly
import pandas as pd
import database as db
from firebase_admin import auth
from admin import admin_bp
import uuid
from sentiment_analyzer import get_sentiment_analysis_for_stock, create_sentiment_visualizations
from logging_config import github_logger
import logging
import traceback
import atexit

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a-super-secret-key-for-local-testing")
app.register_blueprint(admin_bp)

# Initialize logging
github_logger.log_app_start()

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment variables from: {env_path}")
    else:
        print(f"No .env file found at: {env_path}")
except ImportError:
    print("python-dotenv not available, trying to load .env manually...")
    # Manual .env loading as fallback
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"\'')
                    os.environ[key] = value
        print(f"Manually loaded environment variables from: {env_path}")

# Error handling decorator
def log_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            github_logger.log_error(e, f"Route: {request.endpoint}")
            raise
    return decorated_function

# Global error handlers
@app.errorhandler(404)
def not_found_error(error):
    return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404

@app.errorhandler(500)
def internal_error(error):
    github_logger.log_error(error, "Internal Server Error")
    return {'error': 'Internal server error', 'timestamp': str(error)}, 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Don't log 404 errors as exceptions
    if hasattr(e, 'code') and e.code == 404:
        return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404
    
    github_logger.log_error(e, "Unhandled Exception")
    return {'error': 'Application error', 'details': str(e)}, 500

# Log memory usage periodically and push logs to GitHub
def cleanup_and_log():
    github_logger.log_memory_usage()
    github_logger.push_logs_to_github()

atexit.register(cleanup_and_log)

# Ensure Firebase Admin SDK is initialized when the app starts (works under Gunicorn too)
db.initialize_firebase()

# --- Load local company data into memory for searching ---
try:
    company_df = pd.read_csv('indian_stock_tickers.csv')
    company_df['BSE Code'] = company_df['BSE Code'].astype(str).fillna('')
except FileNotFoundError:
    print("[CRITICAL ERROR] The company list 'indian_stock_tickers.csv' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])

# --- Helper function to get an authenticated Supabase client ---
def get_authenticated_client():
    """
    Creates a Supabase client instance for the current user session.
    Prioritizes a full Supabase session, but falls back to a service role client
    if the user is logged in via a Flask session (e.g., email-only).
    """
    access_token = session.get('access_token')
    refresh_token = session.get('refresh_token')
    if access_token and refresh_token:
        sb = db.get_supabase_client()
        try:
            sb.auth.set_session(access_token, refresh_token)
            return sb
        except Exception as e:
            print(f"Session authentication error: {e}")
            # If session is invalid, clear it to force re-login
            session.pop('access_token', None)
            session.pop('refresh_token', None)

    # Fallback for users logged in without a full Supabase session
    if session.get('user_email'):
        return db.get_supabase_client(service_role=True)

    return None

# --- Decorator for Protected Routes ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        sb_client = get_authenticated_client()
        if sb_client is None:
            flash("You must be logged in to view this page.", "warning")
            return redirect(url_for('login'))
        # Pass the authenticated client to the decorated route function
        return f(sb_client, *args, **kwargs)
    return decorated_function

# --- Unified Authentication Logic ---
def _process_firebase_token():
    """Helper function to verify a Firebase token and set the user session."""
    id_token = request.json.get('token')
    if not id_token:
        return jsonify({"success": False, "error": "No token provided."}), 400

    try:
        decoded_token = auth.verify_id_token(id_token)
        user_result = db.find_or_create_supabase_user(decoded_token)

        if user_result.get('error'):
            return jsonify({"success": False, "error": user_result['error']}), 401

        # Set user data that is always present
        session['user_id'] = user_result.get('user_id')
        session['user_phone'] = user_result.get('phone')

        # Handle Supabase session data if it exists
        if session_data := user_result.get('session'):
            session['access_token'] = session_data.get('access_token')
            session['refresh_token'] = session_data.get('refresh_token')
            session['user_email'] = session_data.get('user', {}).get('email') or user_result.get('email')
        else:
            # Fallback for email if no full Supabase session
            session['user_email'] = user_result.get('email')

        # Final check to ensure a user context was established
        if session.get('user_email') or session.get('user_phone'):
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Authentication succeeded but no user context could be established."}), 500

    except Exception as e:
        # Catch specific Firebase auth errors if needed, otherwise generic
        return jsonify({"success": False, "error": f"An unexpected error occurred: {str(e)}"}), 500

# --- Firebase Config Endpoint (for frontend) ---
@app.route('/firebase-config')
def get_firebase_config():
    """Serve Firebase configuration from environment variables."""
    try:
        config = {
            'apiKey': os.environ.get('FIREBASE_API_KEY', ''),
            'authDomain': os.environ.get('FIREBASE_AUTH_DOMAIN', ''),
            'projectId': os.environ.get('FIREBASE_PROJECT_ID', ''),
            'storageBucket': os.environ.get('FIREBASE_STORAGE_BUCKET', ''),
            'messagingSenderId': os.environ.get('FIREBASE_MESSAGING_SENDER_ID', ''),
            'appId': os.environ.get('FIREBASE_APP_ID', '')
        }
        
        # Validate that all required config values are present
        missing_keys = [key for key, value in config.items() if not value]
        if missing_keys:
            return jsonify({
                'error': f'Missing Firebase configuration: {", ".join(missing_keys)}'
            }), 500
        
        return jsonify(config)
    except Exception as e:
        return jsonify({'error': f'Failed to load Firebase config: {str(e)}'}), 500

# --- Authentication Routes ---
@app.route('/login')
def login():
    """Renders the new unified login page."""
    return render_template('login_unified.html')

@app.route('/cron/master')
@log_errors
def cron_master():
    """
    UNIFIED CRON MASTER CONTROLLER - TIMEOUT OPTIMIZED
    
    Returns immediate response to prevent UptimeRobot timeouts.
    Processes jobs efficiently with timeout protection.
    """
    key = request.args.get('key')
    # Support both environment variable and hardcoded key for UptimeRobot compatibility
    expected = os.environ.get('CRON_SECRET_KEY', 'c78b684067c74784364e352c391ecad3')
    if not expected or key != expected:
        return jsonify({
            "status": "unauthorized",
            "message": "Invalid or missing key",
            "timestamp": datetime.now().isoformat()
        }), 401

    # Always use service client for cron
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime, timedelta
        import uuid
        
        # Get current IST time and market status
        now_ist = db.ist_now()
        is_market_hours, market_open, market_close = db.ist_market_window(now_ist)
        is_working_day = now_ist.weekday() < 5  # Monday=0, Friday=4
        
        # Initialize response
        run_id = str(uuid.uuid4())
        results = {
            'timestamp': now_ist.isoformat(),
            'run_id': run_id,
            'market_hours': is_market_hours,
            'working_day': is_working_day,
            'executed_jobs': [],
            'skipped_jobs': [],
            'errors': []
        }
        
        # Job execution flags
        jobs_to_run = []
        
        # 1. BSE ANNOUNCEMENTS - Always run (every 5 minutes, 24/7)
        jobs_to_run.append({
            'name': 'bse_announcements',
            'condition': True,  # Always run
            'reason': 'Continuous monitoring'
        })
        
        # 2. LIVE PRICE MONITORING - Only during market hours on working days
        if is_working_day and is_market_hours:
            jobs_to_run.append({
                'name': 'live_price_monitoring', 
                'condition': True,
                'reason': f'Market hours: {market_open.strftime("%H:%M")} - {market_close.strftime("%H:%M")}'
            })
        else:
            results['skipped_jobs'].append({
                'name': 'live_price_monitoring',
                'reason': f'Outside market hours or non-working day. Market: {is_market_hours}, Working day: {is_working_day}'
            })
        
        # 3. NEWS MONITORING - Every 30 minutes (at :00 and :30)
        # Check if current minute is 0 or 30 (within ±2 minutes for tolerance)
        current_minute = now_ist.minute
        should_run_news = (
            current_minute in range(0, 5) or        # :00-:04 (wider window)
            current_minute in range(25, 35) or      # :25-:34 (wider window for :30)
            current_minute in range(55, 61)         # :55-:00 (wider window)
        )
        
        if should_run_news:
            # Check if already run in the last 25 minutes to prevent duplicates
            last_25_min = now_ist - timedelta(minutes=25)
            try:
                recent_runs = sb.table('cron_run_logs').select('created_at').eq('job', 'news_monitoring').gte('created_at', last_25_min.isoformat()).execute()
                if recent_runs.data:
                    results['skipped_jobs'].append({
                        'name': 'news_monitoring',
                        'reason': 'Already executed within last 25 minutes'
                    })
                else:
                    jobs_to_run.append({
                        'name': 'news_monitoring',
                        'condition': True,
                        'reason': f'30-minute schedule: {now_ist.strftime("%H:%M")} (target: :00/:30)'
                    })
            except Exception:
                # If we can't check, run anyway to be safe
                jobs_to_run.append({
                    'name': 'news_monitoring',
                    'condition': True,
                    'reason': f'30-minute schedule: {now_ist.strftime("%H:%M")} (could not verify recent runs)'
                })
        else:
            results['skipped_jobs'].append({
                'name': 'news_monitoring',
                'reason': f'Not scheduled time. Current: {now_ist.strftime("%H:%M")}, Target: :00/:30 (±2min)'
            })
        
        # 4. DAILY SUMMARY - Once per day at 16:30 (after market close)
        summary_time_target = now_ist.replace(hour=16, minute=30, second=0, microsecond=0)
        time_diff = abs((now_ist - summary_time_target).total_seconds() / 60)  # difference in minutes
        
        # Run daily summary if:
        # - It's a working day
        # - Current time is within 2 minutes of 16:30 (16:28-16:32) - NARROW WINDOW
        # - Haven't run it today already
        should_run_summary = (
            is_working_day and 
            time_diff <= 2 and  # Changed from 10 to 2 minutes
            now_ist >= summary_time_target.replace(minute=28)  # After 16:28
        )
        
        if should_run_summary:
            # Check if already run today - improved duplicate detection
            today_start = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
            try:
                existing_runs = sb.table('cron_run_logs').select('created_at').eq('job', 'daily_summary').gte('created_at', today_start.isoformat()).execute()
                if existing_runs.data:
                    # Check if any run was within the last 30 minutes (much shorter window)
                    for run in existing_runs.data:
                        try:
                            # Handle both timezone-aware and naive datetime strings
                            run_time_str = run['created_at']
                            if run_time_str.endswith('Z'):
                                run_time_str = run_time_str.replace('Z', '+00:00')
                            elif '+' not in run_time_str and 'T' in run_time_str:
                                run_time_str += '+00:00'
                            
                            run_time = datetime.fromisoformat(run_time_str)
                            
                            # Convert to IST for comparison
                            if run_time.tzinfo is not None:
                                run_time_ist = run_time.astimezone(now_ist.tzinfo)
                            else:
                                run_time_ist = run_time.replace(tzinfo=now_ist.tzinfo)
                            
                            time_since_run = (now_ist - run_time_ist).total_seconds()
                            
                            if time_since_run < 7200:  # 2 hours - only one daily summary per day
                                results['skipped_jobs'].append({
                                    'name': 'daily_summary',
                                    'reason': f'Already executed today at {run_time_ist.strftime("%H:%M")} ({time_since_run/60:.0f} min ago)'
                                })
                                should_run_summary = False
                                break
                        except Exception as parse_error:
                            # If we can't parse the time, assume it's old and continue
                            continue
                
                if should_run_summary:  # Still should run
                    jobs_to_run.append({
                        'name': 'daily_summary',
                        'condition': True,
                        'reason': f'Scheduled time reached: {now_ist.strftime("%H:%M")}'
                    })
            except Exception:
                # If we can't check, run anyway to be safe
                jobs_to_run.append({
                    'name': 'daily_summary',
                    'condition': True,
                    'reason': 'Scheduled time reached (could not verify if already run)'
                })
        else:
            results['skipped_jobs'].append({
                'name': 'daily_summary', 
                'reason': f'Not scheduled time. Current: {now_ist.strftime("%H:%M")}, Target: 16:30 (±2min)'
            })
        
        # Execute the jobs
        if not jobs_to_run:
            results['message'] = 'No jobs scheduled for execution'
            return jsonify(results)
        
        # Get user data once
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({
                'bse_code': r.get('bse_code'), 
                'company_name': r.get('company_name')
            })

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'),
                'user_name': r.get('user_name', 'User')
            })
        
        # Execute each job
        daily_summary_sent_users = set()  # Track users who already received daily summary in this run
        
        for job in jobs_to_run:
            job_name = job['name']
            job_result = {
                'name': job_name,
                'reason': job['reason'],
                'users_processed': 0,
                'notifications_sent': 0,
                'users_skipped': 0,
                'errors': []
            }
            
            try:
                for uid, scrips in scrips_by_user.items():
                    recipients = recs_by_user.get(uid) or []
                    if not scrips or not recipients:
                        job_result['users_skipped'] += 1
                        continue
                    
                    # SPECIAL HANDLING FOR DAILY SUMMARY - prevent multiple sends to same user in one run
                    if job_name == 'daily_summary':
                        if uid in daily_summary_sent_users:
                            # Skip this user - already sent daily summary in this run
                            job_result['users_skipped'] += 1
                            continue
                        else:
                            # Mark user as processed for daily summary
                            daily_summary_sent_users.add(uid)
                    
                    try:
                        # Execute appropriate function based on job type
                        if job_name == 'bse_announcements':
                            sent = db.send_bse_announcements_consolidated(sb, uid, scrips, recipients, hours_back=1)
                        elif job_name == 'live_price_monitoring':
                            # Enhanced price spike alerts with debugging and lower thresholds
                            print(f"🔍 PRICE SPIKE: Processing {len(scrips)} scrips for user {uid[:8]}...")
                            from database import ist_market_window
                            is_open, open_dt, close_dt = ist_market_window()
                            print(f"🔍 PRICE SPIKE: Market open: {is_open} (Current time in IST)")
                            
                            if is_open:
                                # Lower thresholds for better detection: 5% price change, 300% volume spike
                                sent = db.send_hourly_spike_alerts(sb, uid, scrips, recipients, price_threshold_pct=5.0, volume_threshold_pct=300.0)
                                print(f"🔍 PRICE SPIKE: Messages sent: {sent}")
                            else:
                                print(f"🔍 PRICE SPIKE: Market closed, skipping alerts")
                                sent = 0
                        elif job_name == 'daily_summary':
                            sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                        elif job_name == 'bulk_deals_monitoring':
                            # Import and use bulk deals monitoring
                            from bulk_deals_monitor import send_bulk_deals_alerts
                            sent = send_bulk_deals_alerts(sb, uid, scrips, recipients)
                        elif job_name == 'news_monitoring':
                            # Import and use RSS news monitoring with duplicate prevention
                            print(f"🔥 RSS NEWS: Starting duplicate-safe news monitoring for user {uid[:8]}...")
                            from simple_rss_fix import send_rss_news_no_duplicates
                            sent = send_rss_news_no_duplicates(sb, uid, scrips, recipients)
                            print(f"🔥 RSS NEWS: Completed - {sent} messages sent")
                        else:
                            continue
                        
                        job_result['users_processed'] += 1
                        job_result['notifications_sent'] += sent
                        
                        # Log individual job execution
                        try:
                            user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                            sb.table('cron_run_logs').insert({
                                'run_id': run_id,
                                'job': job_name,
                                'user_id': user_uuid,
                                'processed': True,
                                'notifications_sent': int(sent),
                                'recipients': int(len(recipients))
                            }).execute()
                        except Exception as log_error:
                            job_result['errors'].append(f"Log error for user {uid}: {log_error}")
                            
                    except Exception as user_error:
                        job_result['errors'].append({"user_id": uid, "error": str(user_error)})
                        job_result['users_skipped'] += 1
                
                results['executed_jobs'].append(job_result)
                
            except Exception as job_error:
                results['errors'].append(f"Job {job_name} failed: {str(job_error)}")
        
        # Return quick response to prevent UptimeRobot timeout
        quick_response = {
            'status': 'success',
            'timestamp': results['timestamp'],
            'run_id': results['run_id'],
            'working_day': results['working_day'],
            'market_hours': results['market_hours'],
            'message': f'Processed {len(results["executed_jobs"])} jobs successfully',
            'executed_jobs': results['executed_jobs'],
            'skipped_jobs': results['skipped_jobs'],
            'errors': results['errors'],
            'response_time': 'optimized'
        }
        
        return jsonify(quick_response)
        
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "timestamp": datetime.now().isoformat()}), 500

@app.route('/cron/bse_announcements')
@app.route('/cron/hourly_spike_alerts')
@app.route('/cron/evening_summary')
@log_errors
def cron_bse_announcements():
    """Cron-compatible endpoint to send BSE announcements.
    Expects a secret key in query string (?key=...) to prevent abuse.
    Optionally accepts hours_back (default 1).

    This endpoint iterates over all users who have both monitored scrips and
    at least one Telegram recipient, and sends consolidated announcements.
    """
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    # Always use service client for cron
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        # Allow overriding hours_back via query param (default: 1 hour)
        try:
            hours_back = int(request.args.get('hours_back', '1'))
        except Exception:
            hours_back = 1

        # Fetch all scrips and recipients once
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0, "recipients": 0, "items": 0}
        errors = []

        import uuid
        run_id = str(uuid.uuid4())
        job_name = 'hourly_spike_alerts' if request.path.endswith('/hourly_spike_alerts') else 'bse_announcements'

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': False,
                        'notifications_sent': 0,
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log skipped cron run: {e}")
                continue
            try:
                # Decide which job to run based on path
                if request.path.endswith('/hourly_spike_alerts'):
                    sent = db.send_hourly_spike_alerts(sb, uid, scrips, recipients)
                elif request.path.endswith('/evening_summary'):
                    # Enforce evening run by default; allow override with force=true
                    force = request.args.get('force') == 'true'
                    is_open, open_dt, close_dt = db.ist_market_window()
                    from datetime import datetime
                    now = db.ist_now()
                    if (now <= close_dt) and not force:
                        # Skip if before or during market hours unless forced
                        sent = 0
                    else:
                        # Send price summary instead of announcements
                        sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                else:
                    sent = db.send_bse_announcements_consolidated(sb, uid, scrips, recipients, hours_back=hours_back)
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                totals["recipients"] += len(recipients)
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log cron run: {e}")
                # We do not know exact items here, but we can log via BSE_VERBOSE in the function
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/verify_phone_token', methods=['POST'])
def verify_phone_token():
    """Endpoint for verifying Firebase phone auth tokens."""
    return _process_firebase_token()

@app.route('/verify_google_token', methods=['POST'])
def verify_google_token():
    """Endpoint for verifying Firebase Google auth tokens."""
    return _process_firebase_token()

@app.route('/logout')
def logout():
    session.clear()
    flash("You have been successfully logged out.", "success")
    return redirect(url_for('login'))

# --- Main Application Routes (Protected) ---
@app.route('/health')
def health_check():
    """Lightweight health check endpoint for uptime monitoring.
    Returns 200 OK with minimal processing to keep the app alive.
    """
    from datetime import datetime
    try:
        # Quick DB connectivity check
        sb = db.get_supabase_client(service_role=True)
        if sb:
            # Very lightweight query
            sb.table('profiles').select('id', count='exact').limit(1).execute()
            db_status = 'connected'
        else:
            db_status = 'disconnected'
    except Exception as e:
        db_status = f'error: {str(e)[:50]}'
    
    return {
        'status': 'ok',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': 'bse-monitor',
        'database': db_status,
        'memory_mb': get_memory_usage()
    }, 200

@app.route('/debug/cron_auth')
def debug_cron_auth():
    """Debug endpoint to check cron authentication"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    
    return {
        'provided_key': key,
        'expected_key': expected,
        'keys_match': key == expected,
        'expected_exists': expected is not None,
        'provided_exists': key is not None,
        'expected_length': len(expected) if expected else 0,
        'provided_length': len(key) if key else 0
    }

@app.route('/debug/user_setup')
@login_required
def debug_user_setup(sb):
    """Debug endpoint to check user's setup"""
    user_id = session.get('user_id')
    
    # Get user's monitored scrips
    monitored_scrips = db.get_user_scrips(sb, user_id)
    
    # Get user's recipients
    recipients = db.get_user_recipients(sb, user_id)
    
    # Get user's category preferences
    category_prefs = db.get_user_category_prefs(sb, user_id)
    
    return {
        'user_id': user_id,
        'monitored_scrips': monitored_scrips,
        'recipients': recipients,
        'category_preferences': category_prefs,
        'scrip_count': len(monitored_scrips),
        'recipient_count': len(recipients),
        'category_count': len(category_prefs)
    }

@app.route('/debug/cron_logs')
def debug_cron_logs():
    """Debug endpoint to check recent cron job runs"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Get recent cron runs (last 50, ordered by id desc since created_at doesn't exist)
        result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(50).execute()
        
        return {
            'success': True,
            'total_runs': len(result.data),
            'recent_runs': result.data
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/test/evening_summary')
def test_evening_summary():
    """Test endpoint to manually trigger evening summary without secret key"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Force run evening summary for all users
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        job_name = 'evening_summary_test'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                # Send price summary instead of announcements
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                users_processed += 1
                notifications_sent += sent
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1

        return {
            'success': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/monitor/cron_status')
def monitor_cron_status():
    """Monitoring dashboard for cron job status and recent runs"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        from datetime import datetime, timedelta
        
        # Get recent cron runs (last 24 hours)
        result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(100).execute()
        
        # Analyze the data
        runs_by_job = {}
        total_notifications = 0
        total_users = 0
        recent_errors = []
        
        for run in result.data:
            job = run.get('job', 'unknown')
            if job not in runs_by_job:
                runs_by_job[job] = {
                    'total_runs': 0,
                    'successful_runs': 0,
                    'total_notifications': 0,
                    'total_users': 0,
                    'last_run': None,
                    'recent_runs': []
                }
            
            runs_by_job[job]['total_runs'] += 1
            runs_by_job[job]['recent_runs'].append(run)
            
            if run.get('processed'):
                runs_by_job[job]['successful_runs'] += 1
                runs_by_job[job]['total_notifications'] += run.get('notifications_sent', 0)
                runs_by_job[job]['total_users'] += 1
            
            # Track the most recent run for each job
            if not runs_by_job[job]['last_run']:
                runs_by_job[job]['last_run'] = run
        
        # Calculate summary stats
        for job_data in runs_by_job.values():
            total_notifications += job_data['total_notifications']
            total_users += job_data['total_users']
            # Keep only last 10 runs for each job
            job_data['recent_runs'] = job_data['recent_runs'][:10]
        
        # Get current IST time and market status
        ist_now = db.ist_now()
        is_market_open, market_open_time, market_close_time = db.ist_market_window()
        
        return {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'ist_time': ist_now.isoformat(),
            'market_status': {
                'is_open': is_market_open,
                'open_time': market_open_time.isoformat() if market_open_time else None,
                'close_time': market_close_time.isoformat() if market_close_time else None
            },
            'summary': {
                'total_jobs': len(runs_by_job),
                'total_notifications_sent': total_notifications,
                'total_user_runs': total_users,
                'total_runs_analyzed': len(result.data)
            },
            'jobs': runs_by_job,
            'quick_links': {
                'test_evening_summary': '/test/evening_summary',
                'debug_cron_logs': '/debug/cron_logs',
                'debug_cron_auth': '/debug/cron_auth',
                'health_check': '/health'
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/force/evening_summary')
def force_evening_summary():
    """Force trigger evening summary bypassing all timing restrictions"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized - Use: /force/evening_summary?key=YOUR_CRON_SECRET_KEY", 403
    
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        job_name = 'evening_summary_forced'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        print(f"FORCE EVENING SUMMARY: Processing {len(scrips_by_user)} users...")

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                # FORCE send price summary - bypass all timing checks
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                users_processed += 1
                notifications_sent += sent
                print(f"  User {uid}: sent {sent} notifications")
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1
                print(f"  ERROR User {uid}: {e}")

        result = {
            'success': True,
            'forced': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'ist_time': db.ist_now().isoformat(),
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
        
        print(f"FORCE EVENING SUMMARY COMPLETE: {result}")
        return result
        
    except Exception as e:
        print(f"FORCE EVENING SUMMARY ERROR: {e}")
        return {'error': str(e)}, 500

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        return 'unknown'

@app.route('/')
@login_required
def dashboard(sb):
    """Main dashboard showing monitored scrips and recipients."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    telegram_recipients = db.get_user_recipients(sb, user_id)
    
    category_prefs = db.get_user_category_prefs(sb, user_id)
    return render_template('dashboard.html', 
                           monitored_scrips=monitored_scrips,
                           telegram_recipients=telegram_recipients,
                           category_prefs=category_prefs,
                           user_email=session.get('user_email', ''),
                           user_phone=session.get('user_phone', ''))

@app.route('/search')
@login_required
def search(sb):
    """Endpoint for fuzzy searching company names and BSE codes."""
    query = request.args.get('query', '')
    if not query or len(query) < 2:
        return jsonify({"matches": []})
    
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
           
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})

@app.route('/send_script_messages', methods=['POST'])
@login_required
def send_script_messages(sb):
    """Triggers sending Telegram messages for all monitored scrips."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        
        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            messages_sent = db.send_script_messages_to_telegram(sb, user_id, monitored_scrips, telegram_recipients)
            if messages_sent > 0:
                flash(f'Successfully sent {messages_sent} message(s)!', 'success')
            else:
                flash('No messages were sent. Check scrips and recipients.', 'info')
            
    except Exception as e:
        flash(f'Error sending messages: {str(e)}', 'error')
        print(f"Error in send_script_messages: {e}")
    
    return redirect(url_for('dashboard'))

@app.route('/send_bse_announcements', methods=['POST'])
@login_required
def send_bse_announcements(sb):
    """Send consolidated BSE announcements for monitored scrips to Telegram recipients."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        hours_back = 24
        try:
            hours_back = int(request.form.get('hours_back', 24))
        except Exception:
            hours_back = 24

        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            sent = db.send_bse_announcements_consolidated(sb, user_id, monitored_scrips, telegram_recipients, hours_back=hours_back)
            if sent > 0:
                flash(f'Sent announcements summary to {sent} recipient(s).', 'success')
            else:
                flash('No new announcements found in the selected period.', 'warning')
    except Exception as e:
        flash(f'Error sending BSE announcements: {str(e)}', 'error')
        print(f"Error in send_bse_announcements: {e}")

    return redirect(url_for('dashboard'))

# --- Data Management Routes (Protected) ---
@app.route('/add_scrip', methods=['POST'])
@login_required
def add_scrip(sb):
    user_id = session.get('user_id')
    bse_code = request.form.get('scrip_code')
    company_name = request.form.get('company_name', '').strip()

    if not bse_code:
        flash('Scrip code is required.', 'error')
        return redirect(url_for('dashboard'))

    if not company_name:
        match = company_df[company_df['BSE Code'] == bse_code]
        if not match.empty:
            company_name = str(match.iloc[0]['Company Name'])
        else:
            flash('Scrip code not found. Please check the BSE code.', 'error')
            return redirect(url_for('dashboard'))

    db.add_user_scrip(sb, user_id, bse_code, company_name)
    flash(f'Added {company_name} to your watchlist.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/delete_scrip', methods=['POST'])
@login_required
def delete_scrip(sb):
    user_id = session.get('user_id')
    bse_code = request.form['scrip_code']
    db.delete_user_scrip(sb, user_id, bse_code)
    flash(f'Scrip {bse_code} removed from your watchlist.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/add_recipient', methods=['POST'])
@login_required
def add_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name', '').strip()
    
    if not user_name:
        flash('Recipient name is required.', 'error')
        return redirect(url_for('dashboard'))
    
    result = db.add_user_recipient(sb, user_id, chat_id, user_name)
    
    if result['success']:
        flash(result['message'], 'success')
    else:
        flash(result['message'], 'error')
    
    return redirect(url_for('dashboard'))

@app.route('/delete_recipient', methods=['POST'])
@login_required
def delete_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name')  # Optional for backwards compatibility
    
    db.delete_user_recipient(sb, user_id, chat_id, user_name)
    
    if user_name:
        flash(f'Recipient "{user_name}" ({chat_id}) removed.', 'success')
    else:
        flash(f'Recipient {chat_id} removed.', 'success')
    
    return redirect(url_for('dashboard'))

@app.route('/set_category_prefs', methods=['POST'])
@login_required
def set_category_prefs(sb):
    user_id = session.get('user_id')
    selected = request.form.getlist('categories')
    ok = db.set_user_category_prefs(sb, user_id, selected)
    if ok:
        flash('Category preferences saved.', 'success')
    else:
        flash('Failed to save preferences.', 'error')
    return redirect(url_for('dashboard'))

# --- Enhanced Sentiment Analysis Routes (Protected) ---
@app.route('/sentiment_analysis_mobile')
@login_required
def sentiment_analysis_mobile(sb):
    """Renders the enhanced mobile-friendly sentiment analysis dashboard."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    return render_template('sentiment_analysis_mobile.html', 
                         scrips=monitored_scrips,
                         user_email=session.get('user_email'))

@app.route('/sentiment_analysis_detailed')
@login_required  
def sentiment_analysis_detailed(sb):
    """Renders the detailed sentiment analysis results page."""
    return render_template('sentiment_analysis_detailed.html')

@app.route('/analyze_sentiment', methods=['POST'])
@login_required
def analyze_sentiment(sb):
    """API endpoint for comprehensive sentiment analysis (on-demand)."""
    try:
        data = request.get_json()
        stock_symbol = data.get('stock_symbol')
        company_name = data.get('company_name')
        
        if not stock_symbol or not company_name:
            return jsonify({'error': 'Stock symbol and company name required'}), 400
        
        # Use comprehensive sentiment analysis service
        from sentiment_analysis_service import perform_comprehensive_sentiment_analysis
        
        # Perform comprehensive analysis (API + Database)
        analysis_result = perform_comprehensive_sentiment_analysis(sb, stock_symbol, company_name)
        
        if analysis_result.get('success'):
            return jsonify({
                'success': True,
                'sentiment_data': analysis_result
            })
        else:
            return jsonify({
                'success': False,
                'error': analysis_result.get('error', 'Analysis failed')
            }), 500
            
    except Exception as e:
        print(f"Error in analyze_sentiment: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_sentiment_summary')
@login_required
def get_sentiment_summary(sb):
    """API endpoint for recent news monitoring status (not sentiment analysis)."""
    try:
        user_id = session.get('user_id')
        monitored_scrips = db.get_user_scrips(sb, user_id)
        
        # Get recent news monitoring status from database
        summary_data = []
        for scrip in monitored_scrips:
            try:
                # Get most recent news for this stock (not sentiment)
                result = sb.table('processed_news_articles')\
                    .select('*')\
                    .eq('stock_query', scrip['company_name'])\
                    .order('processed_at', desc=True)\
                    .limit(1)\
                    .execute()
                
                if result.data:
                    news_item = result.data[0]
                    summary_data.append({
                        'bse_code': scrip['bse_code'],
                        'company_name': scrip['company_name'],
                        'overall_sentiment': 'NEWS_AVAILABLE',
                        'sentiment_score': 0,
                        'confidence': 0,
                        'total_articles': 1,
                        'last_analysis': news_item['processed_at'],
                        'has_data': True
                    })
                else:
                    # No news data yet
                    summary_data.append({
                        'bse_code': scrip['bse_code'],
                        'company_name': scrip['company_name'],
                        'overall_sentiment': 'PENDING',
                        'sentiment_score': 0,
                        'confidence': 0,
                        'total_articles': 0,
                        'has_data': False
                    })
            except Exception as e:
                print(f"Error getting news status for {scrip.get('company_name', 'N/A')}: {e}")
                continue
        
        return jsonify({'success': True, 'summary_data': summary_data})
    except Exception as e:
        print(f"Error in get_sentiment_summary: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/toggle_sentiment_preference', methods=['POST'])
@login_required
def toggle_sentiment_preference(sb):
    """Toggle sentiment analysis preference for a specific stock."""
    try:
        user_id = session.get('user_id')
        data = request.get_json()
        stock_name = data.get('stock_name')
        enabled = data.get('enabled', True)
        
        if not stock_name:
            return jsonify({'error': 'Stock name required'}), 400
        
        # Check if preference already exists
        existing = sb.table('user_sentiment_preferences')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('stock_name', stock_name)\
            .execute()
        
        if existing.data:
            # Update existing preference
            sb.table('user_sentiment_preferences')\
                .update({'sentiment_enabled': enabled, 'updated_at': 'NOW()'})\
                .eq('user_id', user_id)\
                .eq('stock_name', stock_name)\
                .execute()
        else:
            # Create new preference
            sb.table('user_sentiment_preferences').insert({
                'user_id': user_id,
                'stock_name': stock_name,
                'sentiment_enabled': enabled
            }).execute()
        
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error in toggle_sentiment_preference: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_sentiment_preferences')
@login_required
def get_sentiment_preferences(sb):
    """Get sentiment analysis preferences for all user's stocks."""
    try:
        user_id = session.get('user_id')
        monitored_scrips = db.get_user_scrips(sb, user_id)
        
        preferences = {}
        for scrip in monitored_scrips:
            stock_name = scrip['company_name']
            
            # Get preference from database
            result = sb.table('user_sentiment_preferences')\
                .select('sentiment_enabled, min_confidence_threshold')\
                .eq('user_id', user_id)\
                .eq('stock_name', stock_name)\
                .execute()
            
            if result.data:
                pref = result.data[0]
                preferences[stock_name] = {
                    'enabled': pref['sentiment_enabled'],
                    'min_confidence': pref['min_confidence_threshold']
                }
            else:
                # Default preferences
                preferences[stock_name] = {
                    'enabled': True,
                    'min_confidence': 40
                }
        
        return jsonify({'success': True, 'preferences': preferences})
    except Exception as e:
        print(f"Error in get_sentiment_preferences: {e}")
        return jsonify({'error': str(e)}), 500

# --- Health Check ---

# --- Additional Monitoring Endpoints ---
@app.route('/ping')
def ping():
    """Simple ping endpoint for keep-alive monitoring - NO LOGIN REQUIRED"""
    from datetime import datetime
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "message": "pong",
        "server": "stockmonitor-aknr"
    }), 200

@app.route('/uptime')
def uptime():
    """UptimeRobot specific endpoint for keep-alive - NO LOGIN REQUIRED"""
    from datetime import datetime
    return jsonify({
        "status": "up",
        "timestamp": datetime.now().isoformat(),
        "server": "stockmonitor-aknr",
        "message": "Server is alive and preventing Render sleep",
        "purpose": "keep_alive"
    }), 200

# --- Main Execution ---
if __name__ == '__main__':
    db.initialize_firebase()
    port = int(os.environ.get('PORT', os.environ.get('FLASK_RUN_PORT', 5000)))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug)




