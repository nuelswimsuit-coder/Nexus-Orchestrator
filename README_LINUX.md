# Worker Dashboard Kiosk Mode (Linux)

This guide configures a Linux worker laptop to:

1. Never sleep while plugged in.
2. Auto-start the Worker Sentinel Dashboard on boot.

The dashboard script is `scripts/worker_dashboard.py`.

## 1) Install prerequisites

From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure environment

Set Redis and node identity in your shell profile or `.env`:

```bash
export REDIS_URL="redis://<MASTER_IP>:6379/0"
export NODE_ID="worker-linux-01"
export NODE_NAME="Worker Linux 01"
export NEXUS_API_BASE_URL="http://127.0.0.1:8001"
```

## 3) Disable sleep (plugged-in kiosk behavior)

For GNOME systems:

```bash
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type 'nothing'
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-battery-type 'nothing'
gsettings set org.gnome.desktop.session idle-delay 0
```

If `gsettings` is not available, use your distro's power settings UI and set:

- Suspend: Never (AC power)
- Screen blanking: Never

## 4) Create a systemd service for auto-launch

Create `~/.config/systemd/user/telefix-worker-dashboard.service`:

```ini
[Unit]
Description=Telefix Worker Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/Desktop/Nexus-Orchestrator
Environment=PYTHONUNBUFFERED=1
Environment=REDIS_URL=redis://<MASTER_IP>:6379/0
Environment=NODE_ID=worker-linux-01
Environment=NODE_NAME=Worker Linux 01
Environment=NEXUS_API_BASE_URL=http://127.0.0.1:8001
ExecStart=%h/Desktop/Nexus-Orchestrator/.venv/bin/python %h/Desktop/Nexus-Orchestrator/scripts/worker_dashboard.py --refresh 1.0
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
```

Enable and start it:

```bash
systemctl --user daemon-reload
systemctl --user enable telefix-worker-dashboard.service
systemctl --user start telefix-worker-dashboard.service
```

Allow user services to start at boot even without active login:

```bash
sudo loginctl enable-linger "$USER"
```

## 5) Optional: force dashboard on local TTY kiosk screen

If you want the dashboard fullscreen on `tty1`, add this to `~/.bash_profile`:

```bash
if [ -z "$DISPLAY" ] && [ "$(tty)" = "/dev/tty1" ]; then
  cd ~/Desktop/Nexus-Orchestrator
  source .venv/bin/activate
  exec python scripts/worker_dashboard.py --refresh 1.0
fi
```

## 6) Verify

Check service status:

```bash
systemctl --user status telefix-worker-dashboard.service
```

View logs:

```bash
journalctl --user -u telefix-worker-dashboard.service -f
```

You should see a live terminal board with:
- `Node Health`
- `Current Task`
- `AI Thinking`
- `Action History`
