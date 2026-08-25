#!/usr/bin/env python3
"""Crop and magnify a region of a rendered page, for confident reading of small type.

Uses Pillow only — no PyMuPDF, keeping the gold-standard toolchain disjoint from
`corpus`'s.

Usage:
    gold_crop.py <in.png> <out.png> <left> <top> <right> <bottom> [scale]

Bounds are fractions of page width/height in [0,1]. `scale` defaults to 2.0 and
is applied after cropping, with Lanczos resampling.
"""
import sys

from PIL import Image

Image.MAX_IMAGE_PIXELS = None


def main():
    src, dst = sys.argv[1], sys.argv[2]
    l, t, r, b = (float(x) for x in sys.argv[3:7])
    scale = float(sys.argv[7]) if len(sys.argv) > 7 else 2.0
    im = Image.open(src)
    W, H = im.size
    box = (int(l * W), int(t * H), int(r * W), int(b * H))
    if box[2] <= box[0] or box[3] <= box[1]:
        raise SystemExit(f"empty crop box {box} for image {W}x{H}")
    crop = im.crop(box)
    if scale != 1.0:
        crop = crop.resize(
            (int(crop.width * scale), int(crop.height * scale)), Image.LANCZOS
        )
    # keep the long edge inside a sane budget for image-token cost
    MAX = 2200
    if max(crop.size) > MAX:
        f = MAX / max(crop.size)
        crop = crop.resize((int(crop.width * f), int(crop.height * f)), Image.LANCZOS)
    crop.save(dst)
    print(f"{dst}  {crop.width}x{crop.height}  from {box} of {W}x{H}")


if __name__ == "__main__":
    main()
