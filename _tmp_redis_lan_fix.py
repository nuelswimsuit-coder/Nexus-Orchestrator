"""Re-run if Redis restarts: tcp-keepalive; bind/protected-mode not supported via CONFIG on this build."""
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True, socket_connect_timeout=5)
r.ping()
for name, val in (("bind", "0.0.0.0"), ("protected-mode", "no"), ("tcp-keepalive", "300")):
    try:
        r.config_set(name, val)
        print(f"CONFIG SET {name} OK")
    except redis.ResponseError as e:
        print(f"CONFIG SET {name} skipped: {e}")
print("CONFIG GET bind:", r.config_get("bind"))
