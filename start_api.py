import uvicorn
import sys
import os

# הוספת נתיב הפרויקט כדי שפייתון יכיר את nexus
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    print("--- TELEFIX MASTER API AUTHORITY - ONLINE ---")
    # הרצת ה-FastAPI מתוך התיקייה הקיימת
    uvicorn.run("nexus.services.api.main:app", host="0.0.0.0", port=8001, reload=True)
