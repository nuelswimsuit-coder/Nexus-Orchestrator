import os
import subprocess
import time

def launch():
    # הגדרת נתיב עבודה
    os.environ["PYTHONPATH"] = os.getcwd()

    # רשימת הפקודות המדויקת לפי הקבצים בתיקיית scripts שלך
    commands = [
        {"title": "REDIS", "cmd": 'docker run -d --name telefix-redis -p 6379:6379 redis || docker start telefix-redis'},
        {"title": "API", "cmd": 'python scripts/start_api.py'},
        {"title": "BOT", "cmd": 'python scripts/start_telegram_bot.py'},
        {"title": "FRONTEND", "cmd": 'cd frontend && npm run dev'},
        {"title": "WORKER", "cmd": 'python scripts/start_worker.py'}
    ]

    # פתיחת Windows Terminal עם טאבים נפרדים
    wt_cmd = 'wt -p "Command Prompt" '
    parts = []
    for c in commands:
        # פקודת cmd /k שומרת על הטרמינל פתוח גם אם יש שגיאה, כדי שתוכל לראות אותה
        parts.append(f'nt -d . --title "{c["title"]}" cmd /k "{c["cmd"]}"')
    
    final_cmd = wt_cmd + " ; ".join(parts)
    
    print("🚀 TeleFix OS: Booting all systems... Please wait for Docker pull if it's the first time.")
    subprocess.Popen(final_cmd, shell=True)

if __name__ == "__main__":
    launch()