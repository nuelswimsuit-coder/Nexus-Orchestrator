import os
from dotenv import load_dotenv
import ccxt
import requests
import time

load_dotenv()

def test_connections():
    print("🔍 מתחיל בדיקת מערכות עבור Nexus...")
    
    # 1. בדיקת נתיב פרויקטים
    path = os.getenv('NEXUS_PROJECTS_DIR')
    if path and os.path.exists(path):
        print("✅ נתיב פרויקטים תקין.")
    else:
        print(f"❌ נתיב לא נמצא או לא הוגדר: {path}")

    # 2. בדיקת בינאנס עם תיקון זמן אוטומטי
    try:
        exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_API_SECRET'),
            'enableRateLimit': True,
            'options': {
                'adjustForTimeDifference': True,  # פותר את שגיאת ה-Timestamp
                'recvWindow': 10000               # מרחיב את חלון הזמן המותר ל-10 שניות
            }
        })
        
        # ניסיון למשוך נתונים כדי לאמת חיבור
        balance = exchange.fetch_status()
        print(f"✅ חיבור לבינאנס הצליח. סטטוס: {balance['status']}")
    except Exception as e:
        print(f"❌ שגיאה בחיבור לבינאנס: {e}")

    # 3. בדיקת טלגרם
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    try:
        url = f"https://api.telegram.org/bot{token}/getMe"
        res = requests.get(url).json()
        if res.get('ok'):
            print(f"✅ בוט טלגרם מחובר: @{res['result']['username']}")
        else:
            print(f"❌ שגיאה בטוקן של טלגרם: {res.get('description')}")
    except Exception as e:
        print(f"❌ שגיאה בתקשורת מול טלגרם: {e}")

if __name__ == "__main__":
    test_connections()