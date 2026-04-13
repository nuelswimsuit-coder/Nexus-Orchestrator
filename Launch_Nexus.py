"""
Nexus Orchestrator — GUI Launcher
One-click startup for all services. Compiles to .exe via PyInstaller.
"""
from __future__ import annotations

import os, sys, subprocess, threading, time, webbrowser, ctypes
import tkinter as tk
from tkinter import font as tkfont
from pathlib import Path

# ── Windows taskbar: tell Windows this is its own app (shows custom icon) ──
try:
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("NexusOrchestrator.Launcher.1")
except Exception:
    pass

# ── Root directory (works as .py and as compiled .exe) ─────────────────────
ROOT = Path(sys.executable).parent if getattr(sys, "frozen", False) \
       else Path(__file__).resolve().parent

# ── Palette ────────────────────────────────────────────────────────────────
C = dict(
    bg       = "#080b14",
    surface  = "#0d1120",
    surface2 = "#131928",
    border   = "#1a2235",
    accent   = "#00b4ff",
    adim     = "#005a80",
    success  = "#00e096",
    warn     = "#ffb800",
    danger   = "#ff3355",
    text     = "#e8f2ff",
    tdim     = "#4a7a9b",
    tmuted   = "#2d4a65",
)

# ── Services ───────────────────────────────────────────────────────────────
SERVICES = [
    {
        "id":    "redis",
        "label": "Redis",
        "cmd":   "docker start telefix-redis || docker run -d --name telefix-redis -p 6379:6379 redis:7-alpine",
        "shell": True,
    },
    {
        "id":    "master",
        "label": "Nexus Master",
        "cmd":   [sys.executable, "scripts/start_master.py"],
        "shell": False,
    },
    {
        "id":    "api",
        "label": "API Server",
        "cmd":   [sys.executable, "scripts/start_api.py"],
        "shell": False,
    },
    {
        "id":    "worker",
        "label": "Worker Node",
        "cmd":   [sys.executable, "scripts/start_worker.py"],
        "shell": False,
    },
    {
        "id":    "frontend",
        "label": "Dashboard UI",
        "cmd":   "npm run dev",
        "cwd":   str(ROOT / "frontend"),
        "shell": True,
    },
]

# ── Runtime state ──────────────────────────────────────────────────────────
_procs:   dict[str, subprocess.Popen] = {}
_sv:      dict[str, tk.StringVar]     = {}   # status text vars
_dot:     dict[str, tk.Label]         = {}   # coloured dot labels
_log_ref: list[tk.Text]               = []   # single-element list for closure access


def _dot_color(s: str) -> str:
    return {
        "RUNNING":  C["success"],
        "STARTING": C["warn"],
        "STOPPED":  C["tmuted"],
        "ERROR":    C["danger"],
    }.get(s, C["tmuted"])


def _set_status(sid: str, status: str) -> None:
    if sid in _sv:  _sv[sid].set(status)
    if sid in _dot: _dot[sid].config(fg=_dot_color(status))


def _log(text: str, tag: str = "normal") -> None:
    if not _log_ref: return
    w = _log_ref[0]
    w.config(state="normal")
    w.insert("end", text, tag)
    w.see("end")
    w.config(state="disabled")


def _stream(proc: subprocess.Popen, sid: str) -> None:
    try:
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line: continue
            lo = line.lower()
            tag = "error" if any(k in lo for k in ("error","exception","critical","failed")) else \
                  "warn"  if any(k in lo for k in ("warn","warning")) else "tdim"
            _log(f"[{sid.upper():8}] {line}\n", tag)
    except Exception:
        pass


def _launch_one(svc: dict) -> None:
    sid = svc["id"]
    _set_status(sid, "STARTING")
    _log(f"\n▶  {svc['label']}...\n", "accent")
    cwd = svc.get("cwd", str(ROOT))
    try:
        proc = subprocess.Popen(
            svc["cmd"],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=svc.get("shell", False),
            env={**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"},
        )
        _procs[sid] = proc
        time.sleep(1.5 if sid == "redis" else 0.8)

        if proc.poll() is None:
            _set_status(sid, "RUNNING")
            _log(f"  ✓  {svc['label']} running  (PID {proc.pid})\n", "success")
            threading.Thread(target=_stream, args=(proc, sid), daemon=True).start()
        else:
            _set_status(sid, "ERROR")
            _log(f"  ✗  {svc['label']} exited immediately.\n", "error")
    except Exception as exc:
        _set_status(sid, "ERROR")
        _log(f"  ✗  {svc['label']}: {exc}\n", "error")


def _launch_all(btn_launch: tk.Button, btn_stop: tk.Button) -> None:
    btn_launch.config(state="disabled", text="  LAUNCHING…  ", bg=C["warn"], fg=C["bg"])
    btn_stop.config(state="normal")

    def _run():
        for svc in SERVICES:
            _launch_one(svc)
            time.sleep(0.3)
        time.sleep(3)
        _log("\n🌐  Opening Dashboard → http://localhost:3000\n", "accent")
        webbrowser.open("http://localhost:3000")
        btn_launch.config(text="  ● RUNNING  ", bg=C["success"], fg=C["bg"])

    threading.Thread(target=_run, daemon=True).start()


def _stop_all(btn_launch: tk.Button, btn_stop: tk.Button) -> None:
    _log("\n■  Stopping all services…\n", "warn")
    for sid, proc in _procs.items():
        try: proc.terminate(); _set_status(sid, "STOPPED")
        except Exception: pass
    _procs.clear()
    btn_launch.config(state="normal", text="  ▶  LAUNCH ALL  ", bg=C["accent"], fg=C["bg"])
    btn_stop.config(state="disabled")
    _log("  Done.\n", "warn")


# ── Build UI ───────────────────────────────────────────────────────────────
def main() -> None:
    root = tk.Tk()
    root.title("Nexus Orchestrator")
    root.geometry("900x660")
    root.minsize(780, 540)
    root.configure(bg=C["bg"])

    ico = ROOT / "nexus_icon.ico"
    if ico.exists():
        try: root.iconbitmap(str(ico))
        except Exception: pass

    # ── Fonts ──────────────────────────────────────────────────────────────
    families = tkfont.families()
    mono_fam  = "JetBrains Mono" if "JetBrains Mono" in families else "Consolas"
    sans_fam  = "Segoe UI"       if "Segoe UI"       in families else "Arial"

    F = dict(
        title  = tkfont.Font(family=sans_fam, size=20, weight="bold"),
        sub    = tkfont.Font(family=sans_fam, size=9),
        label  = tkfont.Font(family=sans_fam, size=10, weight="bold"),
        btn    = tkfont.Font(family=sans_fam, size=11, weight="bold"),
        small  = tkfont.Font(family=sans_fam, size=8),
        mono   = tkfont.Font(family=mono_fam, size=10),
        mono_s = tkfont.Font(family=mono_fam, size=9),
    )

    # ── Header ─────────────────────────────────────────────────────────────
    hdr = tk.Frame(root, bg=C["surface"], height=72)
    hdr.pack(fill="x")
    hdr.pack_propagate(False)

    tk.Label(hdr, text="NEXUS",        font=F["title"], bg=C["surface"], fg=C["accent"]).place(x=24, y=10)
    tk.Label(hdr, text="ORCHESTRATOR", font=tkfont.Font(family=sans_fam, size=10, weight="bold"),
             bg=C["surface"], fg=C["tdim"]).place(x=26, y=44)
    tk.Label(hdr, text="Distributed Agentic Workflow System",
             font=F["sub"], bg=C["surface"], fg=C["tmuted"]).place(relx=0.5, y=32, anchor="center")
    tk.Label(hdr, text=" v2.0 ", font=F["small"],
             bg=C["adim"], fg=C["text"], padx=5, pady=2).place(relx=1.0, x=-18, y=24, anchor="ne")

    tk.Frame(root, bg=C["border"], height=1).pack(fill="x")

    # ── Body ───────────────────────────────────────────────────────────────
    body = tk.Frame(root, bg=C["bg"])
    body.pack(fill="both", expand=True, padx=20, pady=16)

    # ── Left: service list ─────────────────────────────────────────────────
    left = tk.Frame(body, bg=C["bg"], width=240)
    left.pack(side="left", fill="y", padx=(0, 16))
    left.pack_propagate(False)

    tk.Label(left, text="SERVICES", font=tkfont.Font(family=sans_fam, size=8, weight="bold"),
             bg=C["bg"], fg=C["tmuted"]).pack(anchor="w", pady=(0, 8))

    for svc in SERVICES:
        row = tk.Frame(left, bg=C["surface2"], pady=9, padx=12)
        row.pack(fill="x", pady=3)
        dot = tk.Label(row, text="●", font=tkfont.Font(size=9),
                       bg=C["surface2"], fg=C["tmuted"])
        dot.pack(side="left", padx=(0, 9))
        _dot[svc["id"]] = dot
        tk.Label(row, text=svc["label"], font=F["label"],
                 bg=C["surface2"], fg=C["text"]).pack(side="left")
        sv = tk.StringVar(value="STOPPED")
        _sv[svc["id"]] = sv
        tk.Label(row, textvariable=sv, font=F["small"],
                 bg=C["surface2"], fg=C["tmuted"]).pack(side="right")

    # ── Right: log panel ───────────────────────────────────────────────────
    right = tk.Frame(body, bg=C["bg"])
    right.pack(side="left", fill="both", expand=True)

    tk.Label(right, text="SYSTEM LOG",
             font=tkfont.Font(family=sans_fam, size=8, weight="bold"),
             bg=C["bg"], fg=C["tmuted"]).pack(anchor="w", pady=(0, 6))

    log_frame = tk.Frame(right, bg=C["surface"],
                         highlightthickness=1, highlightbackground=C["border"])
    log_frame.pack(fill="both", expand=True)

    log = tk.Text(log_frame, bg=C["surface"], fg=C["text"], font=F["mono"],
                  state="disabled", wrap="word", bd=0, padx=12, pady=10,
                  cursor="arrow", selectbackground=C["adim"])
    sb  = tk.Scrollbar(log_frame, command=log.yview,
                       bg=C["surface2"], troughcolor=C["surface"], width=8)
    log.configure(yscrollcommand=sb.set)
    sb.pack(side="right", fill="y")
    log.pack(side="left", fill="both", expand=True)
    _log_ref.append(log)

    log.tag_config("normal",  foreground=C["text"])
    log.tag_config("accent",  foreground=C["accent"])
    log.tag_config("success", foreground=C["success"])
    log.tag_config("warn",    foreground=C["warn"])
    log.tag_config("error",   foreground=C["danger"])
    log.tag_config("tdim",    foreground=C["tdim"])

    # ASCII banner
    log.config(state="normal")
    banner = (
        "  ███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗\n"
        "  ████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝\n"
        "  ██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗\n"
        "  ██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║\n"
        "  ██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║\n"
        "  ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝\n\n"
    )
    log.insert("end", banner, "accent")
    log.insert("end", "  Ready. Press  LAUNCH ALL  to start all services.\n\n", "tdim")
    log.config(state="disabled")

    # ── Footer ─────────────────────────────────────────────────────────────
    tk.Frame(root, bg=C["border"], height=1).pack(fill="x")

    footer = tk.Frame(root, bg=C["surface"], height=62)
    footer.pack(fill="x")
    footer.pack_propagate(False)

    tk.Label(footer, text=f"  {ROOT}",
             font=F["small"], bg=C["surface"], fg=C["tmuted"], anchor="w").pack(side="left", padx=12)

    btn_stop = tk.Button(
        footer, text="  ■  STOP ALL  ", font=F["btn"],
        bg=C["surface2"], fg=C["tmuted"], activebackground=C["danger"],
        activeforeground=C["bg"], bd=0, padx=18, pady=8,
        cursor="hand2", state="disabled", relief="flat",
    )
    btn_stop.pack(side="right", padx=(6, 18), pady=10)

    btn_open = tk.Button(
        footer, text="  ⊕ Open Dashboard  ", font=F["btn"],
        bg=C["surface2"], fg=C["tdim"], activebackground=C["adim"],
        activeforeground=C["text"], bd=0, padx=18, pady=8,
        cursor="hand2", relief="flat",
        command=lambda: webbrowser.open("http://localhost:3000"),
    )
    btn_open.pack(side="right", padx=4, pady=10)

    btn_launch = tk.Button(
        footer, text="  ▶  LAUNCH ALL  ", font=F["btn"],
        bg=C["accent"], fg=C["bg"], activebackground=C["adim"],
        activeforeground=C["text"], bd=0, padx=26, pady=8,
        cursor="hand2", relief="flat",
    )
    btn_launch.pack(side="right", padx=4, pady=10)

    btn_launch.config(command=lambda: _launch_all(btn_launch, btn_stop))
    btn_stop.config(command=lambda: _stop_all(btn_launch, btn_stop))

    def _on_close():
        _stop_all(btn_launch, btn_stop)
        time.sleep(0.4)
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
