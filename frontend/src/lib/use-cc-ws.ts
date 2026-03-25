"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { API_BASE } from "@/lib/api";

function toWebSocketUrl(httpBase: string): string {
  if (httpBase.startsWith("https://")) {
    return `wss://${httpBase.slice("https://".length)}/api/cc/ws`;
  }
  if (httpBase.startsWith("http://")) {
    return `ws://${httpBase.slice("http://".length)}/api/cc/ws`;
  }
  return `ws://${httpBase}/api/cc/ws`;
}

export interface CcWsMessage {
  raw: string;
  parsed?: Record<string, unknown>;
}

export function useCcWebSocket(enabled: boolean) {
  const [connected, setConnected] = useState(false);
  const [lastMessages, setLastMessages] = useState<CcWsMessage[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  const push = useCallback((raw: string) => {
    let parsed: Record<string, unknown> | undefined;
    try {
      parsed = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      parsed = undefined;
    }
    setLastMessages((prev) => {
      const next = [{ raw, parsed }, ...prev];
      return next.slice(0, 40);
    });
  }, []);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    const url = toWebSocketUrl(API_BASE.replace(/\/$/, ""));
    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch {
      return;
    }
    wsRef.current = ws;
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);
    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") {
        push(ev.data);
      }
    };
    const ping = window.setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send("ping");
        } catch {
          /* ignore */
        }
      }
    }, 25_000);
    return () => {
      window.clearInterval(ping);
      ws.close();
      wsRef.current = null;
    };
  }, [enabled, push]);

  return { connected, lastMessages };
}
