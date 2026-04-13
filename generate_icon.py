"""
Generates a unique Nexus Orchestrator icon programmatically.
Run once: python generate_icon.py
Outputs: nexus_icon.ico (multi-size: 256, 128, 64, 48, 32, 16)
"""
import math
from PIL import Image, ImageDraw, ImageFilter

def make_frame(size: int) -> Image.Image:
    S = size
    img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    cx, cy = S / 2, S / 2
    r = S / 2

    # ── Background circle ────────────────────────────────────────────────
    d.ellipse([2, 2, S-2, S-2], fill=(8, 11, 20, 255))

    # ── Outer ring (electric blue) ───────────────────────────────────────
    ring_w = max(2, S // 28)
    d.ellipse([ring_w, ring_w, S-ring_w, S-ring_w],
              outline=(0, 180, 255, 220), width=ring_w)

    # ── Hexagon grid nodes ───────────────────────────────────────────────
    # Draw 6 outer nodes on a hex pattern + 1 center
    node_r = max(2, S // 18)
    orbit  = r * 0.52
    for i in range(6):
        angle = math.radians(i * 60 - 30)
        nx = cx + orbit * math.cos(angle)
        ny = cy + orbit * math.sin(angle)
        # Connecting line to center
        line_col = (0, 140, 210, 140)
        d.line([(cx, cy), (nx, ny)], fill=line_col, width=max(1, S // 48))
        # Node dot
        d.ellipse([nx-node_r, ny-node_r, nx+node_r, ny+node_r],
                  fill=(0, 180, 255, 255))

    # ── Connect adjacent outer nodes (hexagon outline) ───────────────────
    points = []
    for i in range(6):
        angle = math.radians(i * 60 - 30)
        points.append((cx + orbit * math.cos(angle),
                        cy + orbit * math.sin(angle)))
    for i in range(6):
        a, b = points[i], points[(i+1) % 6]
        d.line([a, b], fill=(0, 100, 160, 100), width=max(1, S // 64))

    # ── Inner "N" glyph — stylised neural core ───────────────────────────
    # Two vertical bars + diagonal
    bar_w = max(2, S // 16)
    pad   = S * 0.30
    top   = S * 0.26
    bot   = S * 0.74
    left  = pad
    right = S - pad

    glyph_col = (0, 180, 255, 255)
    # Left bar
    d.rectangle([left, top, left + bar_w, bot], fill=glyph_col)
    # Right bar
    d.rectangle([right - bar_w, top, right, bot], fill=glyph_col)
    # Diagonal
    for t in range(int(bot - top)):
        frac = t / (bot - top)
        x = left + bar_w//2 + frac * (right - left - bar_w)
        y = top + t
        d.ellipse([x - bar_w//2, y - bar_w//2,
                   x + bar_w//2, y + bar_w//2], fill=glyph_col)

    # ── Center node ──────────────────────────────────────────────────────
    core = max(3, S // 12)
    d.ellipse([cx-core, cy-core, cx+core, cy+core],
              fill=(8, 11, 20, 255))
    d.ellipse([cx-core+2, cy-core+2, cx+core-2, cy+core-2],
              fill=(0, 212, 255, 255))

    # ── Subtle glow via a blurred copy ───────────────────────────────────
    if S >= 48:
        glow = img.copy().filter(ImageFilter.GaussianBlur(radius=max(1, S//20)))
        img = Image.alpha_composite(glow, img)

    return img


def build_ico():
    sizes = [256, 128, 64, 48, 32, 16]
    frames = [make_frame(s) for s in sizes]
    out = "nexus_icon.ico"
    frames[0].save(
        out,
        format="ICO",
        sizes=[(s, s) for s in sizes],
        append_images=frames[1:],
    )
    print(f"OK Icon saved: {out}  ({len(frames)} sizes: {sizes})")

if __name__ == "__main__":
    build_ico()
