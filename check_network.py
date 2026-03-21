#!/usr/bin/env python3
"""
Diagnostic script to check Redis network configuration.
Checks if redis-server is running and what IP addresses it's listening on.
"""

import psutil
import socket
import sys
from typing import Optional, List, Tuple


def get_lan_ip() -> Optional[str]:
    """Get the primary LAN IP address of this machine."""
    try:
        # Connect to a remote address to determine the local IP
        # This doesn't actually send data, just determines the route
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0)
        try:
            # Connect to a non-routable address (doesn't actually connect)
            s.connect(('10.254.254.254', 1))
            ip = s.getsockname()[0]
        except Exception:
            ip = None
        finally:
            s.close()
        return ip
    except Exception:
        return None


def find_redis_process() -> Optional[psutil.Process]:
    """Find the redis-server process."""
    for proc in psutil.process_iter(['pid', 'name', 'exe']):
        try:
            name = proc.info['name'] or ''
            exe = proc.info['exe'] or ''
            if 'redis-server' in name.lower() or 'redis-server' in exe.lower():
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return None


def get_redis_listening_addresses(proc: psutil.Process) -> List[Tuple[str, int]]:
    """Get the IP addresses and ports that Redis is listening on."""
    listening = []
    try:
        connections = proc.connections(kind='inet')
        for conn in connections:
            if conn.status == psutil.CONN_LISTEN:
                ip = conn.laddr.ip
                port = conn.laddr.port
                if port == 6379:  # Default Redis port
                    listening.append((ip, port))
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return listening


def main():
    print("Checking Redis network configuration...")
    print("-" * 50)
    
    # Check if redis-server is running
    redis_proc = find_redis_process()
    if not redis_proc:
        print("❌ ERROR: redis-server is not running.")
        print("   Please start Redis before running this diagnostic.")
        sys.exit(1)
    
    print(f"✓ redis-server is running (PID: {redis_proc.pid})")
    
    # Get listening addresses
    listening = get_redis_listening_addresses(redis_proc)
    if not listening:
        print("❌ ERROR: Redis is not listening on port 6379.")
        sys.exit(1)
    
    # Analyze listening addresses
    has_localhost = False
    has_wildcard = False
    has_lan_ip = False
    lan_ips = []
    
    for ip, port in listening:
        if ip == '127.0.0.1':
            has_localhost = True
        elif ip == '0.0.0.0':
            has_wildcard = True
        else:
            # Check if it's a LAN IP (not loopback)
            if not ip.startswith('127.') and not ip.startswith('::'):
                has_lan_ip = True
                lan_ips.append(ip)
    
    print(f"✓ Redis is listening on: {', '.join(f'{ip}:{port}' for ip, port in listening)}")
    print()
    
    # Determine the result
    if has_wildcard:
        # Listening on 0.0.0.0 means all interfaces
        lan_ip = get_lan_ip()
        if lan_ip:
            print("✅ SUCCESS: Redis is listening on all interfaces (0.0.0.0)")
            print(f"   Workers should use: {lan_ip}")
        else:
            print("✅ SUCCESS: Redis is listening on all interfaces (0.0.0.0)")
            print("   Workers can use this machine's LAN IP address")
    elif has_lan_ip:
        # Listening on specific LAN IP(s)
        primary_ip = lan_ips[0]
        print(f"✅ SUCCESS: Redis is listening on LAN IP: {primary_ip}")
        print(f"   Workers should use: {primary_ip}")
    elif has_localhost and not (has_wildcard or has_lan_ip):
        # Only listening on localhost
        print("❌ WARNING: Redis is only listening on 127.0.0.1 (localhost)")
        print("   Workers on other machines cannot connect.")
        print()
        print("To fix this, edit redis.windows.conf and change:")
        print("   Line 60: # bind 127.0.0.1")
        print("   To:      bind 0.0.0.0")
        print()
        print("Or to bind to a specific LAN IP:")
        lan_ip = get_lan_ip()
        if lan_ip:
            print(f"   Line 60: # bind 127.0.0.1")
            print(f"   To:      bind {lan_ip}")
        else:
            print("   Line 60: # bind 127.0.0.1")
            print("   To:      bind 0.0.0.0")
        print()
        print("After making the change, restart Redis.")
        sys.exit(1)
    else:
        print("⚠️  WARNING: Unexpected Redis configuration detected.")
        print(f"   Listening on: {', '.join(f'{ip}:{port}' for ip, port in listening)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
