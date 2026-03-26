"""Check all heartbeat keys and TTLs."""
import redis
import json

r = redis.from_url('redis://[::1]:6379/0')

hb_keys = r.keys('nexus:heartbeat:*')
print('All heartbeat keys:')
for k in hb_keys:
    hb = r.get(k)
    obj = json.loads(hb)
    ttl = r.ttl(k)
    role = obj.get('role', '?')
    ts = obj.get('timestamp', '?')
    active = obj.get('active_jobs', '?')
    node_id = obj.get('node_id', '?')
    print(f'  {k.decode()}: role={role}, node={node_id}, ts={ts}, active_jobs={active}, TTL={ttl}s')

# Check nexus_core worker count
print('\nTotal worker heartbeats:', len([k for k in hb_keys]))

# Check nexus:tasks:health-check
health = r.get('nexus:tasks:health-check')
print('\nnexus:tasks:health-check:', health)
