#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera sia PDF che DOCX a partire da:
- slides.csv
- slide_texts.json
- immagini slide

Output:
- PDF orizzontale: Slide -> testo -> Slide -> testo ...
- DOCX orizzontale: Slide -> testo -> page break -> ...

Dipendenze:
    pip install pandas pillow reportlab python-docx
"""

import json
import re
import argparse
from pathlib import Path

import pandas as pd
from PIL import Image
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

from docx import Document
from docx.shared import Inches, Pt
from docx.enum.section import WD_ORIENT
from docx.enum.text import WD_BREAK


# =========================================================
# Utility tempi / testo
# =========================================================

def seconds_to_hms(seconds: float) -> str:
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_final_text(text: str) -> str:
    """
    Pulizia leggera e conservativa del testo finale già corretto da LLM.
    Non fa magie e non prova a riscrivere nulla.
    """
    if text is None:
        return ""
    text = text.replace("\ufeff", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.strip()

    # Pulisce gli spazi ma conserva i paragrafi se presenti
    paragraphs = []
    for p in text.split("\n"):
        p = normalize_whitespace(p)
        if p:
            p = re.sub(r"\s+([,;:.!?])", r"\1", p)
            p = re.sub(r"\(\s+", "(", p)
            p = re.sub(r"\s+\)", ")", p)
            paragraphs.append(p)

    return "\n".join(paragraphs).strip()


# =========================================================
# Lettura input strutturati
# =========================================================

def load_slide_texts_json(json_path: Path) -> dict[int, str]:
    raw = json.loads(json_path.read_text(encoding="utf-8"))

    if not isinstance(raw, dict):
        raise ValueError(f"JSON non valido: root non è un oggetto -> {json_path}")

    slides = raw.get("slides")
    if not isinstance(slides, list):
        raise ValueError(f"JSON non valido: chiave 'slides' mancante o non lista -> {json_path}")

    slide_map: dict[int, str] = {}

    for item in slides:
        if not isinstance(item, dict):
            raise ValueError(f"Elemento slide non valido nel JSON: {item!r}")

        if "slide_index" not in item:
            raise ValueError(f"Elemento slide senza 'slide_index': {item!r}")

        slide_index = int(item["slide_index"])
        text = item.get("text", "")
        if text is None:
            text = ""
        text = clean_final_text(str(text))

        if slide_index in slide_map:
            raise ValueError(f"Slide duplicata nel JSON: {slide_index}")

        slide_map[slide_index] = text

    return slide_map


def build_entries_from_csv_and_json(slides_df: pd.DataFrame, slide_text_map: dict[int, str]):
    slides = slides_df.copy().sort_values("timestamp_sec").reset_index(drop=True)

    required_cols = {"slide_index", "timestamp_sec", "filename"}
    missing = required_cols - set(slides.columns)
    if missing:
        raise ValueError(f"Nel CSV mancano le colonne richieste: {sorted(missing)}")

    entries = []

    timestamps = slides["timestamp_sec"].tolist()
    filenames = slides["filename"].tolist()
    slide_indices = slides["slide_index"].tolist()

    for i in range(len(slides)):
        slide_index = int(slide_indices[i])
        start_t = float(timestamps[i])
        end_t = float(timestamps[i + 1]) if i < len(slides) - 1 else float("inf")
        filename = str(filenames[i])

        text = slide_text_map.get(slide_index, "")

        entries.append({
            "slide_index": slide_index,
            "slide_start": start_t,
            "slide_end": end_t,
            "filename": filename,
            "text": text,
        })

    return entries


# =========================================================
# Wrapping testo PDF
# =========================================================

def wrap_text_to_width(text, font_name, font_size, max_width):
    if not text:
        return []

    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    all_lines = []

    for p in paragraphs:
        words = p.split()
        current = ""

        for word in words:
            test = word if not current else current + " " + word
            width = stringWidth(test, font_name, font_size)

            if width <= max_width:
                current = test
            else:
                if current:
                    all_lines.append(current)

                if stringWidth(word, font_name, font_size) <= max_width:
                    current = word
                else:
                    chunk = ""
                    for ch in word:
                        test_chunk = chunk + ch
                        if stringWidth(test_chunk, font_name, font_size) <= max_width:
                            chunk = test_chunk
                        else:
                            if chunk:
                                all_lines.append(chunk)
                            chunk = ch
                    current = chunk

        if current:
            all_lines.append(current)

        all_lines.append("")

    while all_lines and all_lines[-1] == "":
        all_lines.pop()

    return all_lines


# =========================================================
# PDF helpers
# =========================================================

def fit_image_in_box(img_w, img_h, box_w, box_h):
    scale = min(box_w / img_w, box_h / img_h)
    return img_w * scale, img_h * scale


def draw_footer(c, page_w, page_h, page_num):
    c.setFont("Helvetica", 9)
    c.drawRightString(page_w - 24, 16, str(page_num))


def draw_header(c, page_w, page_h, title_left, title_right=None):
    c.setFont("Helvetica-Bold", 12)
    c.drawString(24, page_h - 20, title_left)
    if title_right:
        c.setFont("Helvetica", 10)
        c.drawRightString(page_w - 24, page_h - 20, title_right)
    c.line(24, page_h - 26, page_w - 24, page_h - 26)


def draw_summary_pages(c, page_w, page_h, entries, pdf_title, start_page_num):
    """
    Disegna l'indice su una o più pagine PDF.
    Restituisce il prossimo page number disponibile.
    """
    page_num = start_page_num

    margin_left = 36
    margin_right = 36
    top_y = page_h - 40
    bottom_y = 32
    line_h = 12

    title_drawn = False
    i = 0
    total = len(entries)

    while i < total:
        draw_header(c, page_w, page_h, pdf_title, "Sommario")
        draw_footer(c, page_w, page_h, page_num)

        y = page_h - 70

        if not title_drawn:
            c.setFont("Helvetica-Bold", 20)
            c.drawString(margin_left, y, pdf_title)
            y -= 25

            c.setFont("Helvetica", 12)
            c.drawString(margin_left, y, f"Numero slide: {len(entries)}")
            y -= 40

            title_drawn = True
        else:
            c.setFont("Helvetica-Bold", 14)
            c.drawString(margin_left, y, "Indice (continuazione)")
            y -= 30

        c.setFont("Helvetica-Bold", 12)
        c.drawString(margin_left, y, "Indice")
        y -= 22

        c.setFont("Helvetica", 9)

        while i < total and y >= bottom_y + line_h:
            entry = entries[i]
            row = (
                f"Slide {entry['slide_index']:>3}   "
                f"[{seconds_to_hms(entry['slide_start'])}]   "
                f"{entry['filename']}"
            )
            c.drawString(margin_left + 12, y, row)
            y -= line_h
            i += 1

        c.showPage()
        page_num += 1

    return page_num


def draw_slide_page(c, page_w, page_h, slide_path, text_lines, slide_num, time_range, page_num):
    draw_header(c, page_w, page_h, f"Slide {slide_num}", time_range)
    draw_footer(c, page_w, page_h, page_num)

    margin_x = 24
    top_y = page_h - 40
    bottom_y = 32

    usable_w = page_w - 2 * margin_x
    usable_h = top_y - bottom_y

    img_area_h = usable_h * 0.70
    gap = 10

    img_box_w = usable_w
    img_box_h = img_area_h

    if slide_path.exists():
        with Image.open(slide_path) as im:
            img_w, img_h = im.size

        draw_w, draw_h = fit_image_in_box(img_w, img_h, img_box_w, img_box_h)

        x_img = margin_x + (img_box_w - draw_w) / 2
        y_img = top_y - draw_h
        c.drawImage(
            ImageReader(str(slide_path)),
            x_img,
            y_img,
            width=draw_w,
            height=draw_h,
            preserveAspectRatio=True,
            mask='auto'
        )
    else:
        c.setFont("Helvetica-Bold", 16)
        c.drawString(margin_x, top_y - 30, f"[Immagine non trovata: {slide_path.name}]")
        y_img = top_y - 60

    font_name = "Helvetica"
    font_size = 11
    line_height = 14
    c.setFont(font_name, font_size)

    text_top_y = y_img - gap
    available_h = text_top_y - bottom_y
    max_lines = max(1, int(available_h // line_height))

    consumed = min(len(text_lines), max_lines)

    y = text_top_y - line_height + 2
    for line in text_lines[:consumed]:
        c.drawString(margin_x, y, line)
        y -= line_height

    return consumed


def draw_text_continuation_page(c, page_w, page_h, text_lines, slide_num, page_num):
    draw_header(c, page_w, page_h, f"Slide {slide_num}", "Continuazione testo")
    draw_footer(c, page_w, page_h, page_num)

    margin_x = 36
    top_y = page_h - 40
    bottom_y = 32

    font_name = "Helvetica"
    font_size = 11
    line_height = 14
    c.setFont(font_name, font_size)

    available_h = top_y - bottom_y
    max_lines = max(1, int(available_h // line_height))

    consumed = min(len(text_lines), max_lines)

    y = top_y - line_height
    for line in text_lines[:consumed]:
        c.drawString(margin_x, y, line)
        y -= line_height

    return consumed


def build_pdf(entries, input_dir: Path, output_pdf: Path):
    page_w, page_h = landscape(A4)
    c = canvas.Canvas(str(output_pdf), pagesize=(page_w, page_h))

    pdf_title = output_pdf.stem.replace("_", " ").replace("-", " ")

    page_num = 1
    page_num = draw_summary_pages(c, page_w, page_h, entries, pdf_title, page_num)

    for entry in entries:
        slide_path = input_dir / entry["filename"]
        raw_text = entry["text"].strip()

        if not raw_text:
            raw_text = "[Nessun testo associato a questa slide]"

        max_text_width = page_w - 72
        lines = wrap_text_to_width(raw_text, "Helvetica", 11, max_text_width)

        time_range = f"{seconds_to_hms(entry['slide_start'])}"

        consumed = draw_slide_page(
            c,
            page_w,
            page_h,
            slide_path=slide_path,
            text_lines=lines,
            slide_num=entry["slide_index"],
            time_range=time_range,
            page_num=page_num
        )
        c.showPage()
        page_num += 1

        remaining = lines[consumed:]

        while remaining:
            consumed2 = draw_text_continuation_page(
                c,
                page_w,
                page_h,
                text_lines=remaining,
                slide_num=entry["slide_index"],
                page_num=page_num
            )
            c.showPage()
            page_num += 1
            remaining = remaining[consumed2:]

    c.save()


# =========================================================
# DOCX helpers
# =========================================================

def set_landscape(document: Document):
    section = document.sections[0]
    section.orientation = WD_ORIENT.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width

    section.top_margin = Inches(0.5)
    section.bottom_margin = Inches(0.5)
    section.left_margin = Inches(0.6)
    section.right_margin = Inches(0.6)


def get_usable_width_inches(document: Document) -> float:
    section = document.sections[0]
    usable = section.page_width - section.left_margin - section.right_margin
    return usable / 914400  # EMU -> inches


def add_docx_summary(document: Document, entries, title: str):
    p = document.add_paragraph()
    r = p.add_run(title)
    r.bold = True
    r.font.size = Pt(18)

    p = document.add_paragraph(f"Numero slide: {len(entries)}")
    p.runs[0].font.size = Pt(11)

    p = document.add_paragraph()
    r = p.add_run("Indice")
    r.bold = True
    r.font.size = Pt(13)

    for entry in entries:
        p = document.add_paragraph(
            f"Slide {entry['slide_index']}  [{seconds_to_hms(entry['slide_start'])}]  {entry['filename']}"
        )
        p.paragraph_format.space_after = Pt(2)
        for run in p.runs:
            run.font.size = Pt(10)

    document.add_page_break()


def add_slide_block_docx(document: Document, slide_num: int, slide_path: Path, text: str, max_image_width_inches: float):
    p = document.add_paragraph()
    r = p.add_run(f"Slide {slide_num}")
    r.bold = True
    r.font.size = Pt(14)

    if slide_path.exists():
        try:
            document.add_picture(str(slide_path), width=Inches(max_image_width_inches))
        except Exception:
            p = document.add_paragraph(f"[Errore nel caricamento immagine: {slide_path.name}]")
            for run in p.runs:
                run.font.size = Pt(10)
    else:
        p = document.add_paragraph(f"[Immagine non trovata: {slide_path.name}]")
        for run in p.runs:
            run.font.size = Pt(10)

    p = document.add_paragraph(text if text else "[Nessun testo associato a questa slide]")
    p.paragraph_format.space_before = Pt(8)
    p.paragraph_format.space_after = Pt(0)
    for run in p.runs:
        run.font.size = Pt(11)

    document.add_paragraph().add_run().add_break(WD_BREAK.PAGE)


def build_docx(entries, input_dir: Path, output_docx: Path):
    doc = Document()
    set_landscape(doc)

    title = output_docx.stem.replace("_", " ").replace("-", " ")
    add_docx_summary(doc, entries, title)

    usable_width = get_usable_width_inches(doc)
    image_width = max(4.0, usable_width - 0.2)

    for entry in entries:
        slide_path = input_dir / entry["filename"]
        raw_text = entry["text"].strip()
        add_slide_block_docx(
            doc,
            slide_num=entry["slide_index"],
            slide_path=slide_path,
            text=raw_text,
            max_image_width_inches=image_width
        )

    doc.save(str(output_docx))


# =========================================================
# Main
# =========================================================

def main():
    parser = argparse.ArgumentParser(
        description="Unisce slides.csv + slide_texts.json + immagini slide in PDF e DOCX."
    )
    parser.add_argument(
        "--input-dir",
        default=".",
        help="Cartella contenente slides.csv e immagini slide",
    )
    parser.add_argument(
        "--csv",
        default="slides.csv",
        help="Nome file CSV delle slide",
    )
    parser.add_argument(
        "--slide-texts",
        required=True,
        help="Path del file JSON con il testo finale per slide",
    )
    parser.add_argument(
        "--output-base",
        default="slides_con_testo",
        help="Nome base output senza estensione",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir.strip()).expanduser().resolve()
    csv_path = input_dir / args.csv
    slide_texts_path = Path(args.slide_texts).expanduser().resolve()

    output_pdf = input_dir / f"{args.output_base}.pdf"
    output_docx = input_dir / f"{args.output_base}.docx"

    if not input_dir.exists():
        raise FileNotFoundError(f"Cartella non trovata: {input_dir}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV non trovato: {csv_path}")

    if not slide_texts_path.exists():
        raise FileNotFoundError(f"JSON slide texts non trovato: {slide_texts_path}")

    slides_df = pd.read_csv(csv_path)

    required_cols = {"slide_index", "timestamp_sec", "filename"}
    missing = required_cols - set(slides_df.columns)
    if missing:
        raise ValueError(f"Nel CSV mancano le colonne richieste: {sorted(missing)}")

    slide_text_map = load_slide_texts_json(slide_texts_path)
    entries = build_entries_from_csv_and_json(slides_df, slide_text_map)

    build_pdf(entries, input_dir, output_pdf)
    build_docx(entries, input_dir, output_docx)

    print(f"PDF creato:  {output_pdf}")
    print(f"DOCX creato: {output_docx}")


if __name__ == "__main__":
    main()