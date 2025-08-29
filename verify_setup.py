#!/usr/bin/env python3
"""
Quick verification script for BSE Stock Monitor production setup.
Run this to verify all essential components are properly configured.
"""

import os
import sys
import importlib.util

def check_file_exists(filepath, description):
    """Check if a file exists and report status."""
    exists = os.path.exists(filepath)
    status = "✅" if exists else "❌"
    print(f"{status} {description}: {filepath}")
    return exists

def check_module_import(module_name):
    """Check if a module can be imported."""
    try:
        __import__(module_name)
        print(f"✅ Module import: {module_name}")
        return True
    except ImportError as e:
        print(f"❌ Module import failed: {module_name} - {e}")
        return False

def check_env_var(var_name, required=True):
    """Check if environment variable is set."""
    value = os.environ.get(var_name)
    if value:
        print(f"✅ Environment variable: {var_name} (set)")
        return True
    else:
        status = "❌" if required else "⚠️"
        req_text = "required" if required else "optional"
        print(f"{status} Environment variable: {var_name} ({req_text}, not set)")
        return not required

def main():
    print("🚀 BSE Stock Monitor - Production Setup Verification")
    print("=" * 60)
    
    # Check essential files
    print("\n📁 Essential Files Check:")
    files_ok = all([
        check_file_exists("app.py", "Main application"),
        check_file_exists("database.py", "Database module"),
        check_file_exists("admin.py", "Admin module"),
        check_file_exists("config.py", "Configuration"),
        check_file_exists("logging_config.py", "Logging configuration"),
        check_file_exists("requirements.txt", "Dependencies"),
        check_file_exists("render.yaml", "Deployment config"),
        check_file_exists("templates/dashboard.html", "Dashboard template"),
        check_file_exists("templates/login_unified.html", "Login template"),
        check_file_exists(".env.example", "Environment template"),
        check_file_exists("README.md", "Documentation"),
    ])
    
    # Check Python modules
    print("\n🐍 Python Module Imports:")
    modules_ok = all([
        check_module_import("flask"),
        check_module_import("supabase"),
        check_module_import("firebase_admin"),
        check_module_import("pandas"),
        check_module_import("requests"),
    ])
    
    # Check environment setup (if .env exists)
    print("\n⚙️ Environment Configuration:")
    env_file_exists = os.path.exists('.env')
    if env_file_exists:
        print("✅ .env file found - checking variables...")
        from dotenv import load_dotenv
        load_dotenv()
        
        env_ok = all([
            check_env_var("SUPABASE_URL"),
            check_env_var("SUPABASE_SERVICE_KEY"),
            check_env_var("TELEGRAM_BOT_TOKEN"),
            check_env_var("CRON_SECRET_KEY"),
            check_env_var("FLASK_SECRET_KEY"),
            check_env_var("FIREBASE_SERVICE_ACCOUNT_JSON"),
            check_env_var("BSE_VERBOSE", required=False),
            check_env_var("GOOGLE_GENERATIVE_AI_API_KEY", required=False),
        ])
    else:
        print("⚠️ .env file not found (copy from .env.example)")
        env_ok = False
    
    # Check optional modules
    print("\n🔧 Optional Features:")
    check_module_import("yfinance")
    check_module_import("google.generativeai")
    check_module_import("psutil")
    
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    if files_ok:
        print("✅ All essential files present")
    else:
        print("❌ Some essential files missing")
    
    if modules_ok:
        print("✅ All required Python modules available")
    else:
        print("❌ Some required Python modules missing")
        print("   Run: pip install -r requirements.txt")
    
    if env_ok:
        print("✅ Environment configuration looks good")
    else:
        print("❌ Environment configuration needs attention")
        print("   Copy .env.example to .env and fill in your values")
    
    overall_status = files_ok and modules_ok and env_ok
    
    if overall_status:
        print("\n🎉 READY FOR DEPLOYMENT!")
        print("   Your BSE Stock Monitor is properly configured.")
        print("   Next steps:")
        print("   1. Deploy to Render.com")
        print("   2. Configure environment variables in Render dashboard")
        print("   3. Set up cron monitoring")
        print("   4. Test the multi-user Telegram features")
    else:
        print("\n⚠️ SETUP INCOMPLETE")
        print("   Please fix the issues above before deploying.")
    
    return 0 if overall_status else 1

if __name__ == "__main__":
    sys.exit(main())