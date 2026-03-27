#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


MIN_BLOCK_DURATION_SEC = 0.15


@dataclass
class SRTBlock:
    index: int
    start_sec: float
    end_sec: float
    text: str


def eprint(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Converte un file SRT in TXT applicando solo logica di deduplicazione "
            "intra-blocco e inter-blocco, senza chunk, slide o altre dipendenze."
        )
    )
    parser.add_argument(
        "--srt",
        required=True,
        help="Percorso del file SRT sorgente.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Percorso del file TXT di output.",
    )
    parser.add_argument(
        "--paragraph-breaks",
        action="store_true",
        help="Inserisce una riga vuota tra i blocchi deduplicati invece di unirli in un unico paragrafo.",
    )
    return parser.parse_args()


def srt_timestamp_to_seconds(ts: str) -> float:
    m = re.match(r"^(\d{2}):(\d{2}):(\d{2}),(\d{3})$", ts.strip())
    if not m:
        raise ValueError(f"Timestamp SRT non valido: {ts!r}")
    hh, mm, ss, ms = map(int, m.groups())
    return hh * 3600 + mm * 60 + ss + ms / 1000.0


def normalize_whitespace(text: str) -> str:
    text = text.replace("\ufeff", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in text.split("\n")]
    lines = [ln for ln in lines if ln]
    joined = " ".join(lines)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined


def parse_srt(path: Path) -> List[SRTBlock]:
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n").strip()

    if not raw:
        raise ValueError(f"SRT vuoto: {path}")

    blocks_raw = re.split(r"\n\s*\n", raw)
    blocks: List[SRTBlock] = []

    ts_line_re = re.compile(
        r"^(\d{2}:\d{2}:\d{2},\d{3})\s+-->\s+(\d{2}:\d{2}:\d{2},\d{3})(?:\s+.*)?$"
    )

    skipped_blocks = 0

    for block_raw in blocks_raw:
        lines = [ln.rstrip() for ln in block_raw.split("\n")]
        lines = [ln for ln in lines if ln.strip() != ""]

        if len(lines) < 2:
            skipped_blocks += 1
            continue

        idx: Optional[int] = None
        ts_line: Optional[str] = None
        text_lines: List[str] = []

        if lines[0].strip().isdigit():
            idx = int(lines[0].strip())
            if len(lines) >= 2 and ts_line_re.match(lines[1].strip()):
                ts_line = lines[1].strip()
                text_lines = lines[2:]
            else:
                skipped_blocks += 1
                continue
        elif ts_line_re.match(lines[0].strip()):
            idx = len(blocks) + 1
            ts_line = lines[0].strip()
            text_lines = lines[1:]
        else:
            skipped_blocks += 1
            continue

        m = ts_line_re.match(ts_line)
        if not m:
            skipped_blocks += 1
            continue

        start_sec = srt_timestamp_to_seconds(m.group(1))
        end_sec = srt_timestamp_to_seconds(m.group(2))
        text = normalize_whitespace("\n".join(text_lines))

        if not text:
            skipped_blocks += 1
            continue

        duration = end_sec - start_sec
        if duration < MIN_BLOCK_DURATION_SEC:
            skipped_blocks += 1
            continue

        blocks.append(
            SRTBlock(
                index=idx,
                start_sec=start_sec,
                end_sec=end_sec,
                text=text,
            )
        )

    if not blocks:
        raise ValueError(f"Nessun blocco SRT valido trovato in {path}")

    eprint(f"[INFO] Blocchi SRT validi: {len(blocks)}")
    if skipped_blocks:
        eprint(f"[INFO] Blocchi SRT scartati: {skipped_blocks}")

    return blocks


def dedupe_adjacent_lines(lines: List[str]) -> List[str]:
    out: List[str] = []
    prev: Optional[str] = None
    for line in lines:
        if line == prev:
            continue
        out.append(line)
        prev = line
    return out


def _tokenize_for_overlap(text: str) -> List[str]:
    text = normalize_whitespace(text).lower()
    return re.findall(r"\w+|[^\w\s]", text, flags=re.UNICODE)


def _find_overlap_token_count(
    prev: str,
    curr: str,
    min_words: int = 2,
    max_words: int = 30,
) -> int:
    prev = normalize_whitespace(prev)
    curr = normalize_whitespace(curr)

    if not prev or not curr:
        return 0

    if curr == prev:
        return len(curr.split())

    prev_words = prev.split()
    curr_words = curr.split()
    max_k = min(len(prev_words), len(curr_words), max_words)

    for k in range(max_k, min_words - 1, -1):
        if prev_words[-k:] == curr_words[:k]:
            return k

    prev_tokens = _tokenize_for_overlap(prev)
    curr_tokens = _tokenize_for_overlap(curr)

    if not prev_tokens or not curr_tokens:
        return 0

    max_t = min(len(prev_tokens), len(curr_tokens), max_words * 3)
    best_words = 0

    for t in range(max_t, 0, -1):
        if prev_tokens[-t:] != curr_tokens[:t]:
            continue

        overlap_text = "".join(
            ch if re.match(r"[^\w\s]", ch, flags=re.UNICODE) else f" {ch}"
            for ch in curr_tokens[:t]
        )
        overlap_text = normalize_whitespace(overlap_text)
        overlap_words = len(overlap_text.split())

        if overlap_words >= min_words:
            best_words = overlap_words
            break

    return best_words


def strip_overlap(prev: str, curr: str, min_words: int = 2, max_words: int = 30) -> str:
    prev = normalize_whitespace(prev)
    curr = normalize_whitespace(curr)

    if not prev or not curr:
        return curr

    overlap_words = _find_overlap_token_count(
        prev,
        curr,
        min_words=min_words,
        max_words=max_words,
    )

    if overlap_words <= 0:
        return curr

    curr_words = curr.split()
    if overlap_words >= len(curr_words):
        return ""

    trimmed = " ".join(curr_words[overlap_words:]).strip()
    return normalize_whitespace(trimmed)


def clean_block_texts(blocks: List[SRTBlock]) -> List[str]:
    raw_lines = [normalize_whitespace(block.text) for block in blocks if block.text.strip()]
    raw_lines = dedupe_adjacent_lines(raw_lines)

    out: List[str] = []
    prev_raw: Optional[str] = None

    for raw_line in raw_lines:
        if not raw_line:
            continue

        if prev_raw is None:
            out.append(raw_line)
            prev_raw = raw_line
            continue

        trimmed = strip_overlap(prev_raw, raw_line, min_words=2, max_words=30)
        if trimmed:
            out.append(trimmed)

        prev_raw = raw_line

    return dedupe_adjacent_lines(out)


def join_texts(texts: List[str], paragraph_breaks: bool = False) -> str:
    texts = [normalize_whitespace(t) for t in texts]
    texts = [t for t in texts if t]

    if not texts:
        return ""

    if paragraph_breaks:
        return "\n\n".join(texts).strip() + "\n"

    return re.sub(r"\s+", " ", " ".join(texts)).strip() + "\n"


def main() -> int:
    args = parse_args()

    srt_path = Path(args.srt)
    output_path = Path(args.output)

    if not srt_path.is_file():
        eprint(f"Errore: SRT non trovato: {srt_path}")
        return 1

    eprint(f"[INFO] Leggo SRT: {srt_path}")
    blocks = parse_srt(srt_path)

    eprint("[INFO] Applico deduplicazione...")
    cleaned_texts = clean_block_texts(blocks)

    final_text = join_texts(cleaned_texts, paragraph_breaks=args.paragraph_breaks)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(final_text, encoding="utf-8")

    eprint(f"[INFO] Blocchi finali dopo deduplica: {len(cleaned_texts)}")
    eprint(f"[OK] Scritto: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
