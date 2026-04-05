#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}

SLIDE_HEADING_RE = re.compile(r"^Slide\s+(\d+)\s*$", re.IGNORECASE)
INDEX_LINE_RE = re.compile(
    r"^Slide\s+(\d+)\s+\[(\d{2}:\d{2}:\d{2})\]\s+(.+?)\s*$",
    re.IGNORECASE,
)


@dataclass
class ParsedSlide:
    slide_index: int
    text_parts: list[str] = field(default_factory=list)
    image_rel_ids: list[str] = field(default_factory=list)

    def final_text(self) -> str:
        parts = [part.strip() for part in self.text_parts if part.strip()]
        return "\n\n".join(parts).strip()


@dataclass
class LegacyDocSet:
    source_docx: Path
    source_pdf: Path | None
    target_docx: Path
    target_pdf: Path


def eprint(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def seconds_from_hms(value: str) -> int:
    hh, mm, ss = [int(piece) for piece in value.split(":")]
    return hh * 3600 + mm * 60 + ss


def timestamp_for_name() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def build_summary_prompt() -> str:
    return """Ti fornisco un file JSON che contiene il testo finale, già ripulito, associato alle slide di una lezione.

Il file JSON contiene una lista di slide con indice e testo.
Devi leggere il contenuto complessivo della lezione e produrre un riassunto finale fedele, chiaro e ben strutturato.

Obiettivo:
- sintetizzare i contenuti principali della lezione
- rendere il testo leggibile e ordinato
- mantenere fedeltà ai contenuti presenti
- NON inventare informazioni non supportate dal testo delle slide

Regole obbligatorie:
- usa solo le informazioni presenti nel file
- non aggiungere esempi, spiegazioni o approfondimenti non presenti o non chiaramente deducibili
- se una parte è ambigua o incompleta, trattala con prudenza
- non citare numeri di slide
- non descrivere la struttura del file JSON
- non inserire premesse del tipo "Ecco il riassunto"
- non usare emoji
- non usare tabelle
- non usare blocchi di codice
- non usare HTML
- non usare link
- non usare citazioni markdown con >
- non usare livelli di heading oltre ##

Formato di output obbligatorio:
- restituisci solo Markdown semplice
- usa questa struttura, nell'ordine indicato:

# Riassunto della lezione

## Panoramica
Un breve testo di 1-2 paragrafi che riassume l'argomento generale della lezione.

## Punti chiave
Una lista puntata con 5-10 punti chiave, formulati in modo chiaro e sintetico.

## Implicazioni cliniche
Un breve testo che evidenzi le principali implicazioni diagnostiche, terapeutiche, prognostiche o decisionali, solo se supportate dal contenuto della lezione.
Se il materiale non consente inferenze cliniche affidabili, scrivi una sezione prudente e sobria, senza inventare indicazioni.

## Conclusione
Un breve paragrafo finale che sintetizza il messaggio complessivo della lezione.

Vincoli stilistici:
- scrivi in italiano
- stile chiaro, scorrevole, sobrio
- evita frasi troppo lunghe
- evita ripetizioni
- niente grassetto inutile
- se usi il grassetto, usalo con parsimonia e solo dentro i paragrafi o i punti elenco

Restituisci esclusivamente il markdown finale.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ricostruisce PDF/DOCX legacy presenti nelle sottocartelle di Rebuild_pdf "
            "usando il DOCX come sorgente strutturale, genera il summary via chatgpt "
            "e produce documenti coerenti con il renderer attuale."
        )
    )
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parent),
        help="Cartella radice che contiene le sottocartelle con i documenti legacy.",
    )
    parser.add_argument(
        "--summary-model",
        default="gpt-5.4",
        help="Modello da usare per il summary finale.",
    )
    parser.add_argument(
        "--summary-prompt-file",
        help="Prompt custom per il summary finale.",
    )
    parser.add_argument(
        "--youtube-url",
        "--youtube-URL",
        dest="youtube_url",
        help="URL YouTube opzionale da mostrare nella cover dei documenti rigenerati.",
    )
    parser.add_argument(
        "--skip-summary",
        action="store_true",
        help="Salta la generazione del summary e rigenera solo i documenti con il testo legacy.",
    )
    parser.add_argument(
        "--keep-workdir",
        action="store_true",
        help="Mantiene la cartella temporanea di ricostruzione dentro ogni sottocartella.",
    )
    parser.add_argument(
        "--folder",
        action="append",
        help="Elabora solo la sottocartella indicata. Puoi ripetere il flag più volte.",
    )
    return parser.parse_args()


def find_target_folders(root: Path, selected: Iterable[str] | None) -> list[Path]:
    if selected:
        folders = [root / name for name in selected]
    else:
        folders = [
            path for path in root.iterdir()
            if path.is_dir()
            and not path.name.startswith(".")
            and path.name != "__pycache__"
        ]

    return sorted(
        path for path in folders
        if path.is_dir()
        and not path.name.startswith(".")
        and path.name != "__pycache__"
    )


def locate_legacy_docs(folder: Path) -> list[LegacyDocSet]:
    current_docx = sorted(
        path for path in folder.glob("*.docx")
        if not path.name.startswith("~$")
        and not path.name.startswith("OLD_")
    )
    current_pdf = sorted(
        path for path in folder.glob("*.pdf")
        if not path.name.startswith("OLD_")
    )
    old_docx = sorted(
        path for path in folder.glob("OLD_*.docx")
        if not path.name.startswith("~$")
    )
    old_pdf = sorted(folder.glob("OLD_*.pdf"))

    if not current_docx and not old_docx:
        raise FileNotFoundError(f"Nessun DOCX legacy trovato in {folder}")

    current_docx_map = {path.name: path for path in current_docx}
    current_pdf_map = {path.name: path for path in current_pdf}
    old_docx_map = {path.name.removeprefix("OLD_"): path for path in old_docx}
    old_pdf_map = {path.name.removeprefix("OLD_"): path for path in old_pdf}

    target_docx_names = sorted(set(current_docx_map) | set(old_docx_map))
    docsets: list[LegacyDocSet] = []

    for target_name in target_docx_names:
        target_docx = current_docx_map.get(target_name, folder / target_name)
        source_docx = old_docx_map.get(target_name, current_docx_map.get(target_name))
        if source_docx is None:
            continue

        target_pdf_name = f"{Path(target_name).stem}.pdf"
        target_pdf = current_pdf_map.get(target_pdf_name, folder / target_pdf_name)
        source_pdf = old_pdf_map.get(target_pdf_name, current_pdf_map.get(target_pdf_name))

        docsets.append(
            LegacyDocSet(
                source_docx=source_docx,
                source_pdf=source_pdf,
                target_docx=target_docx,
                target_pdf=target_pdf,
            )
        )

    if not docsets:
        raise FileNotFoundError(f"Nessun DOCX legacy valido trovato in {folder}")

    return docsets


def parse_docx_structure(docx_path: Path) -> tuple[str, list[str], list[ParsedSlide], dict[str, str], zipfile.ZipFile]:
    archive = zipfile.ZipFile(docx_path)
    document_xml = ET.fromstring(archive.read("word/document.xml"))
    rels_xml = ET.fromstring(archive.read("word/_rels/document.xml.rels"))

    rel_map: dict[str, str] = {}
    for rel in rels_xml.findall("pr:Relationship", NS):
        rel_id = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        if rel_id and target:
            rel_map[rel_id] = target

    body = document_xml.find("w:body", NS)
    if body is None:
        raise ValueError(f"Body DOCX non trovato: {docx_path}")

    prelude_lines: list[str] = []
    slides: list[ParsedSlide] = []
    current_slide: ParsedSlide | None = None

    for child in body:
        if child.tag != f"{{{NS['w']}}}p":
            continue

        text = "".join(node.text or "" for node in child.findall(".//w:t", NS))
        text = text.replace("\u00a0", " ").replace("\u2003", " ").strip()
        image_rel_ids = [
            blip.attrib.get(f"{{{NS['r']}}}embed", "")
            for blip in child.findall(".//a:blip", NS)
            if blip.attrib.get(f"{{{NS['r']}}}embed")
        ]

        heading_match = SLIDE_HEADING_RE.match(text)
        if heading_match:
            if current_slide is not None:
                slides.append(current_slide)
            current_slide = ParsedSlide(slide_index=int(heading_match.group(1)))
            if image_rel_ids:
                current_slide.image_rel_ids.extend(image_rel_ids)
            continue

        if current_slide is None:
            if text:
                prelude_lines.append(text)
            continue

        if image_rel_ids:
            current_slide.image_rel_ids.extend(image_rel_ids)

        if text:
            current_slide.text_parts.append(text)

    if current_slide is not None:
        slides.append(current_slide)

    if not prelude_lines:
        raise ValueError(f"Prelude DOCX vuoto o non parsabile: {docx_path}")
    if not slides:
        raise ValueError(f"Nessuna slide trovata nel DOCX: {docx_path}")

    title = prelude_lines[0]
    return title, prelude_lines, slides, rel_map, archive


def parse_index_lines(prelude_lines: list[str]) -> tuple[int | None, dict[int, dict[str, str]]]:
    declared_total = None
    index_entries: dict[int, dict[str, str]] = {}

    for line in prelude_lines:
        if line.lower().startswith("numero slide:"):
            match = re.search(r"(\d+)", line)
            if match:
                declared_total = int(match.group(1))
            continue

        match = INDEX_LINE_RE.match(line)
        if not match:
            continue

        slide_index = int(match.group(1))
        timestamp_hms = match.group(2)
        filename = match.group(3).strip()
        index_entries[slide_index] = {
            "timestamp_hms": timestamp_hms,
            "timestamp_sec": str(seconds_from_hms(timestamp_hms)),
            "filename": filename,
        }

    if not index_entries:
        raise ValueError("Indice legacy non trovato nel prelude del DOCX")

    return declared_total, index_entries


def write_slides_csv(csv_path: Path, slides: list[ParsedSlide], index_entries: dict[int, dict[str, str]]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["slide_index", "timestamp_sec", "timestamp_hms", "filename"],
        )
        writer.writeheader()
        for slide in slides:
            meta = index_entries.get(slide.slide_index)
            if meta is None:
                raise ValueError(f"Slide {slide.slide_index} assente nell'indice legacy")
            writer.writerow({
                "slide_index": slide.slide_index,
                "timestamp_sec": meta["timestamp_sec"],
                "timestamp_hms": meta["timestamp_hms"],
                "filename": meta["filename"],
            })


def write_slide_texts_json(json_path: Path, base_name: str, slides: list[ParsedSlide]) -> None:
    payload = {
        "base_name": base_name,
        "total_slides": len(slides),
        "slides": [
            {
                "slide_index": slide.slide_index,
                "text": slide.final_text(),
            }
            for slide in slides
        ],
    }
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def extract_images(
    archive: zipfile.ZipFile,
    rel_map: dict[str, str],
    slides: list[ParsedSlide],
    index_entries: dict[int, dict[str, str]],
    output_dir: Path,
) -> None:
    for slide in slides:
        if not slide.image_rel_ids:
            continue

        rel_id = slide.image_rel_ids[0]
        target = rel_map.get(rel_id)
        if not target:
            continue

        if target.startswith("/"):
            internal_path = target.lstrip("/")
        else:
            internal_path = f"word/{target}"

        try:
            data = archive.read(internal_path)
        except KeyError:
            continue

        filename = index_entries[slide.slide_index]["filename"]
        (output_dir / filename).write_bytes(data)


def ensure_chatgpt_available() -> None:
    if shutil.which("chatgpt") is None:
        raise FileNotFoundError("Comando 'chatgpt' non trovato nel PATH")


def run_summary_generation(
    slide_texts_json: Path,
    summary_md: Path,
    summary_raw_json: Path,
    model: str,
    prompt: str,
) -> None:
    ensure_chatgpt_available()

    upload = subprocess.run(
        ["chatgpt", "--upload-file", str(slide_texts_json)],
        capture_output=True,
        text=True,
        check=True,
    )
    response = json.loads(upload.stdout)
    file_id = response.get("id")
    if not file_id:
        raise ValueError("Impossibile estrarre file_id dalla risposta di upload")

    subprocess.run(
        [
            "chatgpt",
            "--no-resume",
            "-o",
            str(summary_md),
            "--file-id",
            file_id,
            "-m",
            model,
            "--save-raw",
            str(summary_raw_json),
            prompt,
        ],
        check=True,
    )


def choose_old_name(path: Path) -> Path:
    candidate = path.with_name(f"OLD_{path.name}")
    if not candidate.exists():
        return candidate
    return path.with_name(f"OLD_{timestamp_for_name()}_{path.name}")


def cleanup_pycache_dirs(repo_root: Path, legacy_root: Path) -> None:
    cleanup_roots = [
        legacy_root,
        repo_root / "modules",
    ]

    seen: set[Path] = set()

    for base in cleanup_roots:
        if not base.exists():
            continue
        for path in sorted(base.rglob("__pycache__")):
            if path.is_dir() and path not in seen:
                shutil.rmtree(path, ignore_errors=True)
                seen.add(path)
                eprint(f"[CLEANUP] Rimossa cartella tecnica: {path}")


def invoke_renderer(
    repo_root: Path,
    slides_dir: Path,
    slide_texts_json: Path,
    summary_md: Path | None,
    output_base: str,
    youtube_url: str = "",
) -> tuple[Path, Path]:
    venv_python = repo_root / ".venv" / "bin" / "python"
    python_bin = str(venv_python if venv_python.exists() else Path(sys.executable))
    renderer = repo_root / "modules" / "slides_and_texts_to_pdf.py"

    cmd = [
        python_bin,
        str(renderer),
        "--input-dir",
        str(slides_dir),
        "--csv",
        "slides.csv",
        "--slide-texts",
        str(slide_texts_json),
        "--output-base",
        output_base,
    ]

    if summary_md is not None and summary_md.exists():
        cmd.extend(["--summary-file", str(summary_md)])

    if youtube_url:
        cmd.extend(["--youtube-url", youtube_url])

    subprocess.run(cmd, check=True)
    return slides_dir / f"{output_base}.pdf", slides_dir / f"{output_base}.docx"


def rebuild_folder(
    folder: Path,
    repo_root: Path,
    summary_model: str,
    summary_prompt: str,
    skip_summary: bool,
    keep_workdir: bool,
    youtube_url: str,
) -> None:
    eprint(f"[INFO] Cartella: {folder}")
    docsets = locate_legacy_docs(folder)

    for docset in docsets:
        source_docx = docset.source_docx
        source_pdf = docset.source_pdf
        target_docx = docset.target_docx
        target_pdf = docset.target_pdf

        eprint(f"[INFO] Documento: {target_docx.name}")
        title, prelude_lines, slides, rel_map, archive = parse_docx_structure(source_docx)
        declared_total, index_entries = parse_index_lines(prelude_lines)

        if declared_total is not None and declared_total != len(index_entries):
            archive.close()
            raise ValueError(
                f"Numero slide dichiarato ({declared_total}) diverso dall'indice parsato ({len(index_entries)})"
            )

        slide_indices = [slide.slide_index for slide in slides]
        if sorted(slide_indices) != sorted(index_entries):
            archive.close()
            raise ValueError("Le slide del DOCX e le slide dell'indice non coincidono")

        temp_parent = folder if keep_workdir else None
        with tempfile.TemporaryDirectory(prefix=".rebuild_", dir=temp_parent) as tmp:
            tmp_dir = Path(tmp)
            target_stem = target_docx.stem
            slides_dir = tmp_dir / f"{target_stem} slides"
            slides_dir.mkdir(parents=True, exist_ok=True)

            slide_texts_json = tmp_dir / f"{target_stem}.slide_texts.json"
            summary_md = tmp_dir / f"{target_stem}.summary.md"
            summary_raw_json = tmp_dir / f"{target_stem}.summary.raw.json"

            extract_images(archive, rel_map, slides, index_entries, slides_dir)
            write_slides_csv(slides_dir / "slides.csv", slides, index_entries)
            write_slide_texts_json(slide_texts_json, target_stem, slides)

            if skip_summary:
                summary_path = None
            else:
                run_summary_generation(
                    slide_texts_json=slide_texts_json,
                    summary_md=summary_md,
                    summary_raw_json=summary_raw_json,
                    model=summary_model,
                    prompt=summary_prompt,
                )
                summary_path = summary_md

            new_pdf_tmp, new_docx_tmp = invoke_renderer(
                repo_root=repo_root,
                slides_dir=slides_dir,
                slide_texts_json=slide_texts_json,
                summary_md=summary_path,
                output_base=target_stem,
                youtube_url=youtube_url,
            )

            old_docx_name = source_docx
            old_pdf_name = source_pdf

            if source_docx == target_docx:
                old_docx_name = choose_old_name(source_docx)
                source_docx.rename(old_docx_name)
            elif target_docx.exists():
                target_docx.unlink()

            if source_pdf is not None and source_pdf == target_pdf:
                old_pdf_name = choose_old_name(source_pdf)
                source_pdf.rename(old_pdf_name)
            elif target_pdf.exists():
                target_pdf.unlink()

            shutil.move(str(new_docx_tmp), str(target_docx))
            shutil.move(str(new_pdf_tmp), str(target_pdf))

            eprint(f"[OK] Rigenerati: {target_pdf} | {target_docx}")

        archive.close()
        if old_pdf_name is not None:
            eprint(f"[OK] Legacy rinominati: {old_pdf_name.name} | {old_docx_name.name}")
        else:
            eprint(f"[OK] Legacy DOCX rinominato: {old_docx_name.name}")


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    root = Path(args.root).expanduser().resolve()

    if not root.is_dir():
        eprint(f"Errore: cartella root non trovata: {root}")
        return 1

    if args.summary_prompt_file:
        summary_prompt = Path(args.summary_prompt_file).expanduser().read_text(encoding="utf-8")
    else:
        summary_prompt = build_summary_prompt()

    folders = find_target_folders(root, args.folder)
    if not folders:
        eprint("Errore: nessuna sottocartella da elaborare")
        return 1

    failures = 0

    for folder in folders:
        try:
            rebuild_folder(
                folder=folder,
                repo_root=repo_root,
                summary_model=args.summary_model,
                summary_prompt=summary_prompt,
                skip_summary=args.skip_summary,
                keep_workdir=args.keep_workdir,
                youtube_url=(args.youtube_url or "").strip(),
            )
        except Exception as exc:
            failures += 1
            eprint(f"[ERRORE] {folder.name}: {exc}")

    if failures:
        cleanup_pycache_dirs(repo_root, root)
        eprint(f"[DONE] Completato con {failures} cartelle fallite.")
        return 1

    cleanup_pycache_dirs(repo_root, root)
    eprint("[DONE] Tutte le cartelle elaborate correttamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
