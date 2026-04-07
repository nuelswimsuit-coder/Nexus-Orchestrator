"""
Fast local-ish vision tagging for meme/media ingestion.

Backends (``NEXUS_MEME_VISION_BACKEND``):
  ollama   — HTTP API to a local Ollama server (vision model).
  onnx     — ``onnxruntime`` + classifier ONNX (see env vars below).
  heuristic — always available; coarse layout / colour cues (not semantic).

Returns topic + sentiment labels from a small controlled vocabulary so Redis
indexes stay stable for swarm routing.
"""

from __future__ import annotations

import base64
import json
import math
import os
from pathlib import Path
from typing import Any, Final

import httpx
import structlog
from PIL import Image

log = structlog.get_logger(__name__)

_TOPICS: Final = ("news", "comedy", "politics", "meme", "reaction", "sports", "other")
_SENTIMENTS: Final = ("positive", "negative", "neutral", "mixed")


def _norm_label(raw: str, allowed: tuple[str, ...], default: str) -> str:
    s = (raw or "").strip().lower().replace(" ", "_")
    return s if s in allowed else default


def _heuristic_classify(path: Path) -> dict[str, str]:
    """Cheap CPU-only cues — useful when no ML stack is installed."""
    topic = "other"
    sentiment = "neutral"
    try:
        with Image.open(path) as im:
            im = im.convert("RGB")
            im.thumbnail((256, 256))
            px = list(im.getdata())
        if not px:
            return {"topic": topic, "sentiment": sentiment, "vision_backend": "heuristic"}

        rs = [p[0] for p in px]
        gs = [p[1] for p in px]
        bs = [p[2] for p in px]
        n = float(len(px))
        mr, mg, mb = sum(rs) / n, sum(gs) / n, sum(bs) / n

        # Grayscale spread ~ contrast; channel deltas ~ saturation proxy
        lum = [0.299 * r + 0.587 * g + 0.114 * b for r, g, b in px]
        mean_l = sum(lum) / n
        var_l = sum((x - mean_l) ** 2 for x in lum) / n
        std_l = math.sqrt(var_l)

        warm = mr + mg * 0.5
        cool = mb + mg * 0.3
        if warm > cool * 1.15 and mean_l > 95:
            sentiment = "positive"
        elif mean_l < 55 and std_l > 35:
            sentiment = "negative"
        elif std_l > 45:
            sentiment = "mixed"

        # Edge density (simple horizontal diff on subsampled grid)
        w, h = im.size
        edge_hits = 0
        samples = 0
        for y in range(1, h - 1, max(1, h // 32)):
            for x in range(1, w - 1, max(1, w // 32)):
                i = y * w + x
                j = y * w + (x - 1)
                if i < len(lum) and j < len(lum):
                    edge_hits += abs(lum[i] - lum[j]) > 18
                    samples += 1
        edge_ratio = (edge_hits / samples) if samples else 0.0

        if edge_ratio > 0.42 and std_l > 28:
            topic = "meme"
        elif std_l < 18 and edge_ratio < 0.2:
            topic = "news"
        elif mean_l > 200 and edge_ratio < 0.25:
            topic = "reaction"
        else:
            topic = "other"

        return {"topic": topic, "sentiment": sentiment, "vision_backend": "heuristic"}
    except Exception as exc:
        log.debug("meme_heuristic_vision_failed", path=str(path), error=str(exc))
        return {"topic": "other", "sentiment": "neutral", "vision_backend": "heuristic"}


async def _ollama_classify(path: Path) -> dict[str, str]:
    host = (os.getenv("NEXUS_MEME_OLLAMA_HOST") or "http://127.0.0.1:11434").rstrip("/")
    model = (os.getenv("NEXUS_MEME_OLLAMA_MODEL") or "llava:7b").strip()
    raw = path.read_bytes()
    b64 = base64.b64encode(raw).decode("ascii")
    prompt = (
        "You label Israeli Telegram-style media for a bot index. "
        "Reply with ONE JSON object only, no markdown. Keys: "
        f'"topic" (one of: {", ".join(_TOPICS)}), '
        f'"sentiment" (one of: {", ".join(_SENTIMENTS)}). '
        "Infer topic from visual style (meme vs screenshot vs photo). "
        "Infer sentiment from mood (not political stance)."
    )
    payload: dict[str, Any] = {
        "model": model,
        "stream": False,
        "messages": [
            {
                "role": "user",
                "content": prompt,
                "images": [b64],
            }
        ],
    }
    timeout = float(os.getenv("NEXUS_MEME_OLLAMA_TIMEOUT_S", "120") or "120")
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(f"{host}/api/chat", json=payload)
        r.raise_for_status()
        data = r.json()
    text = (
        (data.get("message") or {}).get("content")
        or data.get("response")
        or ""
    ).strip()
    topic, sentiment = "other", "neutral"
    try:
        # model sometimes wraps in ```json
        if "```" in text:
            text = text.split("```", 2)[1]
            if text.lower().startswith("json"):
                text = text[4:]
        obj = json.loads(text)
        if isinstance(obj, dict):
            topic = _norm_label(str(obj.get("topic", "")), _TOPICS, "other")
            sentiment = _norm_label(str(obj.get("sentiment", "")), _SENTIMENTS, "neutral")
    except Exception as exc:
        log.debug("meme_ollama_json_parse_failed", error=str(exc), preview=text[:200])
    return {"topic": topic, "sentiment": sentiment, "vision_backend": "ollama"}


def _onnx_classify_sync(path: Path) -> dict[str, str]:
    labels_raw = (os.getenv("NEXUS_MEME_ONNX_LABELS") or "").strip()
    labels = tuple(x.strip() for x in labels_raw.split(",") if x.strip())
    if not labels:
        labels = tuple(_TOPICS)
    model_path = (os.getenv("NEXUS_MEME_ONNX_MODEL") or "").strip()
    if not model_path or not Path(model_path).is_file():
        return _heuristic_classify(path)

    import numpy as np  # type: ignore[import-untyped]
    import onnxruntime as ort  # type: ignore[import-untyped]

    session = ort.InferenceSession(
        model_path,
        providers=["CPUExecutionProvider"],
    )
    in_name = session.get_inputs()[0].name
    shp = session.get_inputs()[0].shape
    # Expect NCHW with spatial 224 or None
    def _parse_spatial(dim: Any) -> int:
        if isinstance(dim, int) and dim > 0:
            return dim
        return 224

    h = _parse_spatial(shp[2] if len(shp) > 2 else 224)
    w = _parse_spatial(shp[3] if len(shp) > 3 else 224)

    with Image.open(path) as im:
        im = im.convert("RGB").resize((w, h), Image.Resampling.BILINEAR)
        arr = np.asarray(im).astype("float32") / 255.0
    # NHWC → NCHW, ImageNet-ish normalize (safe default for many ONNX exports)
    mean = np.array([0.485, 0.456, 0.406], dtype="float32")
    std = np.array([0.229, 0.224, 0.225], dtype="float32")
    chw = (arr - mean) / std
    chw = np.transpose(chw, (2, 0, 1))
    batch = np.expand_dims(chw, axis=0)
    out = session.run(None, {in_name: batch})[0]
    logits = np.squeeze(out)
    idx = int(np.argmax(logits))
    raw_topic = labels[idx % len(labels)]
    topic = _norm_label(raw_topic, _TOPICS, "other")
    # Single-head classifiers: keep sentiment neutral unless a second output exists.
    sentiment = "neutral"
    return {"topic": topic, "sentiment": sentiment, "vision_backend": "onnx"}


async def classify_meme_image(path: Path) -> dict[str, str]:
    """
    Classify a local image (path) into ``topic``, ``sentiment``, ``vision_backend``.
    Runs CPU-heavy ONNX work in a thread.
    """
    backend = (os.getenv("NEXUS_MEME_VISION_BACKEND") or "heuristic").strip().lower()
    if backend == "ollama":
        try:
            return await _ollama_classify(path)
        except Exception as exc:
            log.warning("meme_ollama_vision_failed", error=str(exc))
            return _heuristic_classify(path)
    if backend == "onnx":
        import asyncio

        try:
            return await asyncio.to_thread(_onnx_classify_sync, path)
        except Exception as exc:
            log.warning("meme_onnx_vision_failed", error=str(exc))
            return _heuristic_classify(path)
    return _heuristic_classify(path)


async def classify_meme_visual(path: Path, *, media_kind: str) -> dict[str, str]:
    """
    ``media_kind`` is ``photo``, ``video``, ``sticker``, etc. Video uses first-frame
    JPEG when ``ffmpeg`` produced ``path`` already as image; otherwise skips to heuristic
    by probing image open.
    """
    if media_kind == "video" and path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp"}:
        return {"topic": "other", "sentiment": "neutral", "vision_backend": "skipped_video"}
    try:
        with Image.open(path) as im:
            im.verify()
    except Exception:
        return {"topic": "other", "sentiment": "neutral", "vision_backend": "unreadable"}
    return await classify_meme_image(path)
