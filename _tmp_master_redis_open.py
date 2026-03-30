"""One-shot: open Redis for LAN workers (run on Master)."""
import sys

import redis

def main() -> None:
    hosts = ("127.0.0.1", "[::1]")
    last_err = None
    r = None
    for h in hosts:
        try:
            r = redis.Redis(host=h.strip("[]") if h.startswith("[") else h, port=6379, decode_responses=True)
            r.ping()
            break
        except Exception as e:
            last_err = e
            r = None
    if r is None:
        print("Redis connect failed:", last_err, file=sys.stderr)
        sys.exit(1)
    for opt, val in (
        ("bind", "0.0.0.0"),
        ("protected-mode", "no"),
        ("tcp-keepalive", "300"),
        ("timeout", "0"),
    ):
        r.config_set(opt, val)
        print(f"CONFIG SET {opt} {val} -> OK")
    bind_info = r.config_get("bind")
    print("CONFIG GET bind:", bind_info)
    print("✅ MASTER READY: All gates are open for remote workers.")


if __name__ == "__main__":
    main()
