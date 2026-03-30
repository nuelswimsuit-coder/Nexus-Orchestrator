"""One-shot: open Redis for LAN workers (run on Master)."""
import sys

import redis
from redis.exceptions import ResponseError


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    hosts = ("127.0.0.1", "::1")
    last_err = None
    r = None
    for h in hosts:
        try:
            r = redis.Redis(host=h, port=6379, decode_responses=True)
            r.ping()
            break
        except Exception as e:
            last_err = e
            r = None
    if r is None:
        print("Redis connect failed:", last_err, file=sys.stderr)
        sys.exit(1)
    settings = (
        ("bind", "0.0.0.0"),
        ("protected-mode", "no"),
        ("tcp-keepalive", "300"),
        ("timeout", "0"),
    )
    for opt, val in settings:
        try:
            r.config_set(opt, val)
            print(f"CONFIG SET {opt} {val} -> OK")
        except ResponseError as e:
            print(f"CONFIG SET {opt} {val} -> skipped: {e}", file=sys.stderr)
    try:
        bind_info = r.config_get("bind")
    except ResponseError as e:
        bind_info = f"(unavailable: {e})"
    print("CONFIG GET bind:", bind_info)
    print("✅ MASTER READY: All gates are open for remote workers.")


if __name__ == "__main__":
    main()
