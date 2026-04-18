#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


DEFAULT_MODEL = "whisper-1"
DEFAULT_LANGUAGE = "it"
DEFAULT_TARGET_SIZE_MB = 24
DEFAULT_AUDIO_BITRATE = "24k"
DEFAULT_MAX_CHUNK_DURATION_SEC = 900.0
MIN_CHUNK_DURATION_SEC = 300.0


def eprint(*args: object, **kwargs: object) -> None:
    print(*args, file=sys.stderr, **kwargs)


def die(message: str, code: int = 1) -> None:
    eprint(f"[ERRORE] {message}")
    raise SystemExit(code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Estrae audio da un file video, lo ottimizza per STT e genera "
            "un file SRT usando OpenAI Whisper."
        )
    )
    parser.add_argument("--input-video", required=True, help="File video sorgente.")
    parser.add_argument("--output-srt", required=True, help="Percorso SRT finale.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Modello OpenAI STT.")
    parser.add_argument("--language", default=DEFAULT_LANGUAGE, help="Lingua attesa.")
    parser.add_argument(
        "--target-size-mb",
        type=float,
        default=DEFAULT_TARGET_SIZE_MB,
        help="Dimensione massima prudenziale per singolo upload API.",
    )
    parser.add_argument(
        "--audio-bitrate",
        default=DEFAULT_AUDIO_BITRATE,
        help="Bitrate audio ffmpeg per il file ottimizzato e gli eventuali chunk.",
    )
    parser.add_argument(
        "--max-chunk-duration-sec",
        type=float,
        default=DEFAULT_MAX_CHUNK_DURATION_SEC,
        help=(
            "Durata massima per singolo chunk audio. "
            "Serve a evitare upload troppo lunghi anche quando il file resta sotto il limite dimensionale."
        ),
    )
    return parser.parse_args()


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        die(f"Comando non trovato: {name}")


def run(cmd: Sequence[str]) -> None:
    eprint(f"[CMD] {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        die(f"Comando fallito con exit code {exc.returncode}: {' '.join(cmd)}")


def run_capture(cmd: Sequence[str]) -> str:
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        suffix = f" | stderr: {stderr}" if stderr else ""
        die(f"Comando fallito con exit code {exc.returncode}: {' '.join(cmd)}{suffix}")
    return result.stdout.strip()


def ffprobe_duration_seconds(path: Path) -> float:
    raw = run_capture(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
    )
    try:
        duration = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"Durata ffprobe non valida per {path}: {raw!r}") from exc
    if duration <= 0:
        raise RuntimeError(f"Durata non valida per {path}: {duration}")
    return duration


def format_timestamp(seconds: float) -> str:
    seconds = max(0.0, seconds)
    millis = int(round(seconds * 1000.0))
    hours, rem = divmod(millis, 3600 * 1000)
    minutes, rem = divmod(rem, 60 * 1000)
    secs, ms = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{ms:03d}"


def normalize_text(text: str) -> str:
    return " ".join(text.replace("\r", " ").replace("\n", " ").split()).strip()


def get_file_signature(path: Path) -> Dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_workspace_manifest(
    input_video: Path,
    output_srt: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    return {
        "input_video": get_file_signature(input_video),
        "output_srt": str(output_srt),
        "model": args.model,
        "language": args.language,
        "target_size_mb": args.target_size_mb,
        "audio_bitrate": args.audio_bitrate,
        "max_chunk_duration_sec": args.max_chunk_duration_sec,
    }


def ensure_workspace(workspace_dir: Path, manifest: Dict[str, Any]) -> Path:
    workspace_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = workspace_dir / "manifest.json"

    if manifest_path.is_file():
        try:
            existing_manifest = load_json(manifest_path)
        except Exception:
            existing_manifest = {}
        if existing_manifest != manifest:
            eprint("[INFO] Workspace Whisper esistente non compatibile con l'input corrente: pulizia cache locale.")
            shutil.rmtree(workspace_dir)
            workspace_dir.mkdir(parents=True, exist_ok=True)

    save_json(manifest_path, manifest)
    return manifest_path


def encode_optimized_audio(input_video: Path, output_audio: Path, bitrate: str) -> None:
    audio_filter = "highpass=f=80,lowpass=f=7600,loudnorm=I=-16:TP=-1.5:LRA=11"
    run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_video),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-af",
            audio_filter,
            "-c:a",
            "libopus",
            "-b:a",
            bitrate,
            "-vbr",
            "on",
            "-compression_level",
            "10",
            str(output_audio),
        ]
    )


def build_chunk_plan(
    audio_path: Path,
    target_bytes: int,
    max_chunk_duration_sec: float,
) -> List[Tuple[float, float]]:
    size_bytes = audio_path.stat().st_size
    total_duration = ffprobe_duration_seconds(audio_path)

    planned_duration = total_duration

    if size_bytes > target_bytes:
        duration_ratio = target_bytes / float(size_bytes)
        size_limited_duration = max(
            MIN_CHUNK_DURATION_SEC,
            math.floor(total_duration * duration_ratio * 0.92),
        )
        planned_duration = min(planned_duration, size_limited_duration)

    if max_chunk_duration_sec > 0:
        planned_duration = min(
            planned_duration,
            max(MIN_CHUNK_DURATION_SEC, max_chunk_duration_sec),
        )

    planned_duration = min(planned_duration, total_duration)

    if planned_duration >= total_duration:
        return [(0.0, total_duration)]

    chunks: List[Tuple[float, float]] = []
    start = 0.0
    while start < total_duration:
        remaining = total_duration - start
        duration = planned_duration if remaining > planned_duration else remaining
        chunks.append((start, duration))
        start += duration

    return chunks


def encode_chunk(
    optimized_audio: Path,
    chunk_path: Path,
    start_sec: float,
    duration_sec: float,
    bitrate: str,
    target_bytes: int,
) -> None:
    current_duration = duration_sec
    while True:
        run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{start_sec:.3f}",
                "-t",
                f"{current_duration:.3f}",
                "-i",
                str(optimized_audio),
                "-ac",
                "1",
                "-ar",
                "16000",
                "-c:a",
                "libopus",
                "-b:a",
                bitrate,
                "-vbr",
                "on",
                "-compression_level",
                "10",
                str(chunk_path),
            ]
        )

        if chunk_path.stat().st_size <= target_bytes:
            return

        reduced_duration = current_duration * 0.85
        if reduced_duration < MIN_CHUNK_DURATION_SEC:
            die(
                "Impossibile ridurre il chunk sotto il limite API mantenendo una durata utile. "
                "Serve abbassare ulteriormente il bitrate o gestire più chunk."
            )
        current_duration = reduced_duration


def response_to_dict(response: Any) -> Dict[str, Any]:
    if hasattr(response, "model_dump"):
        return response.model_dump()
    if isinstance(response, dict):
        return response
    if hasattr(response, "to_dict"):
        return response.to_dict()
    try:
        return json.loads(str(response))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Risposta OpenAI non decodificabile") from exc


def transcribe_file(client: Any, path: Path, model: str, language: str, prompt: str) -> Dict[str, Any]:
    eprint(f"[INFO] Upload chunk audio: {path.name} ({path.stat().st_size / (1024 * 1024):.2f} MB)")
    with path.open("rb") as audio_file:
        response = client.audio.transcriptions.create(
            file=audio_file,
            model=model,
            language=language,
            prompt=prompt or None,
            response_format="verbose_json",
            timestamp_granularities=["segment"],
        )
    return response_to_dict(response)


def transcribe_file_cached(
    client: Any,
    path: Path,
    cache_path: Path,
    model: str,
    language: str,
    prompt: str,
) -> Dict[str, Any]:
    if cache_path.is_file():
        eprint(f"[INFO] Transcript chunk già presente, skip API: {cache_path.name}")
        return load_json(cache_path)

    payload = transcribe_file(
        client=client,
        path=path,
        model=model,
        language=language,
        prompt=prompt,
    )
    save_json(cache_path, payload)
    return payload


def extract_segments(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    segments = payload.get("segments")
    if isinstance(segments, list) and segments:
        return segments

    text = normalize_text(str(payload.get("text", "")))
    if text:
        return [{"start": 0.0, "end": 0.0, "text": text}]

    raise RuntimeError("Risposta Whisper priva di segments e text")


def build_srt_entries(
    chunks: List[Tuple[Path, float]],
    transcript_cache_dir: Path,
    client: Any,
    model: str,
    language: str,
) -> List[Tuple[float, float, str]]:
    entries: List[Tuple[float, float, str]] = []
    prompt_tail = ""

    for chunk_path, offset_sec in chunks:
        cache_path = transcript_cache_dir / f"{chunk_path.stem}.verbose.json"
        payload = transcribe_file_cached(
            client=client,
            path=chunk_path,
            cache_path=cache_path,
            model=model,
            language=language,
            prompt=prompt_tail,
        )
        segments = extract_segments(payload)
        chunk_full_text: List[str] = []

        for segment in segments:
            text = normalize_text(str(segment.get("text", "")))
            if not text:
                continue

            try:
                start_sec = float(segment.get("start", 0.0))
                end_sec = float(segment.get("end", start_sec))
            except (TypeError, ValueError):
                start_sec = 0.0
                end_sec = start_sec

            if end_sec <= start_sec:
                end_sec = start_sec + 2.0

            entries.append((offset_sec + start_sec, offset_sec + end_sec, text))
            chunk_full_text.append(text)

        prompt_tail = normalize_text(" ".join(chunk_full_text))[-800:]

    return entries


def write_srt(path: Path, entries: List[Tuple[float, float, str]]) -> None:
    lines: List[str] = []
    for idx, (start_sec, end_sec, text) in enumerate(entries, start=1):
        lines.extend(
            [
                str(idx),
                f"{format_timestamp(start_sec)} --> {format_timestamp(end_sec)}",
                text,
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()

    input_video = Path(args.input_video).expanduser().resolve()
    output_srt = Path(args.output_srt).expanduser().resolve()

    if not input_video.is_file():
        die(f"File video non trovato: {input_video}")

    if not os.environ.get("OPENAI_API_KEY"):
        die("OPENAI_API_KEY non impostata")

    require_command("ffmpeg")
    require_command("ffprobe")

    try:
        from openai import OpenAI
    except ImportError as exc:
        raise SystemExit(
            "[ERRORE] Modulo Python 'openai' non installato nella virtualenv. "
            "Aggiorna requirements e ricrea/aggiorna la .venv."
        ) from exc

    output_srt.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = int(args.target_size_mb * 1024 * 1024)

    workspace_dir = output_srt.parent / "slidescribe_whisper" / output_srt.stem
    manifest = build_workspace_manifest(input_video=input_video, output_srt=output_srt, args=args)
    ensure_workspace(workspace_dir=workspace_dir, manifest=manifest)

    optimized_audio = workspace_dir / f"{input_video.stem}.optimized.webm"
    chunks_dir = workspace_dir / "chunks"
    transcripts_dir = workspace_dir / "transcripts"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    transcripts_dir.mkdir(parents=True, exist_ok=True)

    if optimized_audio.is_file() and optimized_audio.stat().st_size > 0:
        eprint(f"[INFO] Audio ottimizzato già presente, skip encoding: {optimized_audio.name}")
    else:
        eprint("[INFO] Estrazione e ottimizzazione audio per STT...")
        encode_optimized_audio(input_video=input_video, output_audio=optimized_audio, bitrate=args.audio_bitrate)

    chunk_plan = build_chunk_plan(
        optimized_audio,
        target_bytes=target_bytes,
        max_chunk_duration_sec=args.max_chunk_duration_sec,
    )
    eprint(f"[INFO] Chunk audio previsti: {len(chunk_plan)}")

    chunk_plan_payload = {
        "chunk_plan": [
            {"index": idx, "start_sec": start_sec, "duration_sec": duration_sec}
            for idx, (start_sec, duration_sec) in enumerate(chunk_plan, start=1)
        ]
    }
    save_json(workspace_dir / "chunk_plan.json", chunk_plan_payload)

    chunk_files: List[Tuple[Path, float]] = []
    for idx, (start_sec, duration_sec) in enumerate(chunk_plan, start=1):
        chunk_path = chunks_dir / f"chunk_{idx:03d}.webm"
        if chunk_path.is_file() and chunk_path.stat().st_size > 0 and chunk_path.stat().st_size <= target_bytes:
            eprint(f"[INFO] Chunk audio già presente, skip encoding: {chunk_path.name}")
        else:
            encode_chunk(
                optimized_audio=optimized_audio,
                chunk_path=chunk_path,
                start_sec=start_sec,
                duration_sec=duration_sec,
                bitrate=args.audio_bitrate,
                target_bytes=target_bytes,
            )
        chunk_files.append((chunk_path, start_sec))

    client = OpenAI()
    entries = build_srt_entries(
        chunks=chunk_files,
        transcript_cache_dir=transcripts_dir,
        client=client,
        model=args.model,
        language=args.language,
    )

    if not entries:
        die("Whisper non ha restituito segmenti utili per generare l'SRT")

    write_srt(output_srt, entries)

    eprint(f"[OK] SRT generato: {output_srt}")


if __name__ == "__main__":
    main()
