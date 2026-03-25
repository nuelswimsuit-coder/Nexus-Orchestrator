"""
Proxy Rotator — manages a list of residential proxies and rotates them
per session. Supports HTTP/SOCKS5 format.

Proxy list format (one per line), any of:
    host:port
    host:port:user:pass
    socks5://user:pass@host:port
    http://user:pass@host:port
"""

from __future__ import annotations

import itertools
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass
class Proxy:
    scheme: str        # "socks5" or "http"
    host: str
    port: int
    username: str = ""
    password: str = ""

    def to_telethon(self) -> dict:
        """Returns kwargs dict for TelegramClient(proxy=...)"""
        import socks
        proxy_type = socks.SOCKS5 if self.scheme == "socks5" else socks.HTTP
        d: dict = {"proxy_type": proxy_type, "addr": self.host, "port": self.port, "rdns": True}
        if self.username:
            d["username"] = self.username
            d["password"] = self.password
        return d

    def __str__(self) -> str:
        auth = f"{self.username}:***@" if self.username else ""
        return f"{self.scheme}://{auth}{self.host}:{self.port}"


def _parse_line(line: str) -> Proxy | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Full URI with @: socks5://user:pass@host:port  or  http://user:pass@host:port
    uri_match = re.match(
        r"(socks5|http)://(?:([^:@]+):([^@]*)@)?([^:]+):(\d+)", line, re.I
    )
    if uri_match:
        scheme, user, pwd, host, port = uri_match.groups()
        return Proxy(scheme.lower(), host, int(port), user or "", pwd or "")

    # URI without @: socks5://host:port:user:pass  or  http://host:port:user:pass
    uri_plain_match = re.match(r"(socks5|http)://([^:]+):(\d+):([^:]+):(.*)", line, re.I)
    if uri_plain_match:
        scheme, host, port, user, pwd = uri_plain_match.groups()
        return Proxy(scheme.lower(), host, int(port), user, pwd)

    # If line contains "://" it was a URI that didn't match — skip it
    if "://" in line:
        return None

    # Plain: host:port  or  host:port:user:pass
    parts = line.split(":")
    if len(parts) == 2:
        return Proxy("socks5", parts[0], int(parts[1]))
    if len(parts) == 4:
        return Proxy("socks5", parts[0], int(parts[1]), parts[2], parts[3])

    return None


class ProxyRotator:
    def __init__(self, proxies: list[Proxy], mode: str = "round_robin") -> None:
        """
        mode: "round_robin" | "random"
        """
        self._proxies = proxies
        self._mode = mode
        self._cycle: Iterator[Proxy] = itertools.cycle(proxies) if proxies else iter([])

    @classmethod
    def from_file(cls, path: str | Path, mode: str = "round_robin") -> "ProxyRotator":
        p = Path(path)
        if not p.exists():
            return cls([], mode)
        proxies = []
        for line in p.read_text(encoding="utf-8").splitlines():
            proxy = _parse_line(line)
            if proxy:
                proxies.append(proxy)
        return cls(proxies, mode)

    @classmethod
    def from_list(cls, lines: list[str], mode: str = "round_robin") -> "ProxyRotator":
        proxies = [p for line in lines if (p := _parse_line(line))]
        return cls(proxies, mode)

    @property
    def count(self) -> int:
        return len(self._proxies)

    @property
    def available(self) -> bool:
        return len(self._proxies) > 0

    def next(self) -> Proxy | None:
        if not self._proxies:
            return None
        if self._mode == "random":
            return random.choice(self._proxies)
        return next(self._cycle)

    def next_telethon(self) -> dict | None:
        p = self.next()
        return p.to_telethon() if p else None
