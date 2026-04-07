#!/bin/sh
# Open TCP 6379 for the Nexus / Swarm Redis broker on the LAN.
# Run on the Linux host that runs Docker Redis (or any master where ufw/iptables applies).
# Restricts source to 10.100.102.0/24 by default; override with FIREWALL_REDIS_SOURCE_CIDR.
#
# Usage (as root):
#   sudo sh ./setup_host_firewall.sh
#   sudo FIREWALL_REDIS_SOURCE_CIDR=10.100.102.0/24 sh ./setup_host_firewall.sh

set -eu

CIDR="${FIREWALL_REDIS_SOURCE_CIDR:-10.100.102.0/24}"
PORT=6379

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

if [ "$(id -u)" -ne 0 ]; then
	die "Run as root (e.g. sudo sh $0)"
fi

if command -v ufw >/dev/null 2>&1; then
	if ufw status | grep -q "Status: active"; then
		printf 'ufw: allowing %s -> tcp/%s\n' "$CIDR" "$PORT"
		ufw allow from "$CIDR" to any port "$PORT" proto tcp
		ufw status numbered || true
	else
		printf 'ufw is installed but inactive. Enable with: sudo ufw enable\n'
		printf 'Adding rule anyway (will apply when ufw is enabled)...\n'
		ufw allow from "$CIDR" to any port "$PORT" proto tcp
	fi
	exit 0
fi

if command -v iptables >/dev/null 2>&1; then
	RULE_SPEC="-p tcp -s $CIDR --dport $PORT -j ACCEPT"
	if iptables -C INPUT $RULE_SPEC 2>/dev/null; then
		printf 'iptables: rule already present for %s -> tcp/%s\n' "$CIDR" "$PORT"
	else
		printf 'iptables: appending INPUT rule for %s -> tcp/%s\n' "$CIDR" "$PORT"
		iptables -A INPUT $RULE_SPEC
	fi
	printf 'Tip: persist rules with your distro (e.g. iptables-persistent, netfilter-persistent).\n'
	exit 0
fi

die "Neither ufw nor iptables found in PATH; install one or configure the firewall manually."
