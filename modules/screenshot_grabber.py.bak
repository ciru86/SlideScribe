#!/usr/bin/env python3

import os
import csv
import math
import argparse
from dataclasses import dataclass
from typing import List, Optional, Tuple

import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim

Point = Tuple[int, int]
RectROI = Tuple[int, int, int, int]
QuadROI = np.ndarray  # shape (4, 2), float32 ordered TL, TR, BR, BL


@dataclass
class SlideCapture:
    index: int
    time_sec: float
    filename: str
    image: np.ndarray


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def format_timestamp(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int(round((seconds - int(seconds)) * 1000))
    if ms == 1000:
        s += 1
        ms = 0
    return f"{h:02d}-{m:02d}-{s:02d}_{ms:03d}"


def default_output_dir_from_video(video_path: str) -> str:
    base = os.path.splitext(os.path.basename(video_path))[0]
    return f"{base} slides"


def preprocess(img: np.ndarray, max_width: int = 1000) -> np.ndarray:
    h, w = img.shape[:2]
    if w > max_width:
        scale = max_width / w
        img = cv2.resize(
            img,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_AREA,
        )
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)
    return gray


def compare_images(img1: np.ndarray, img2: np.ndarray) -> dict:
    a = preprocess(img1)
    b = preprocess(img2)

    h = min(a.shape[0], b.shape[0])
    w = min(a.shape[1], b.shape[1])
    a = a[:h, :w]
    b = b[:h, :w]

    ssim_score = float(ssim(a, b))
    diff = cv2.absdiff(a, b)
    mean_diff = float(np.mean(diff))

    _, thresh = cv2.threshold(diff, 20, 255, cv2.THRESH_BINARY)
    changed_ratio = float(np.count_nonzero(thresh)) / thresh.size

    return {
        "ssim": ssim_score,
        "mean_diff": mean_diff,
        "changed_ratio": changed_ratio,
    }


def is_slide_change(
    metrics: dict,
    ssim_threshold: float,
    mean_diff_threshold: float,
    changed_ratio_threshold: float,
) -> bool:
    return (
        metrics["ssim"] < ssim_threshold
        and (
            metrics["mean_diff"] > mean_diff_threshold
            or metrics["changed_ratio"] > changed_ratio_threshold
        )
    )


def save_image(img: np.ndarray, out_dir: str, index: int, time_sec: float) -> str:
    ts = format_timestamp(time_sec)
    filename = os.path.join(out_dir, f"slide_{index:03d}_{ts}.png")
    ok = cv2.imwrite(filename, img)
    if not ok:
        raise RuntimeError(f"Impossibile salvare il file: {filename}")
    return filename


def write_csv(records: List[SlideCapture], out_csv: str) -> None:
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["slide_index", "timestamp_sec", "timestamp_hms", "filename"])
        for rec in records:
            writer.writerow([
                rec.index,
                f"{rec.time_sec:.3f}",
                format_timestamp(rec.time_sec),
                os.path.basename(rec.filename),
            ])


def clamp_roi(frame: np.ndarray, roi: RectROI) -> Tuple[int, int, int, int]:
    h, w = frame.shape[:2]
    x, y, rw, rh = roi
    x1 = max(0, min(x, w - 1))
    y1 = max(0, min(y, h - 1))
    x2 = max(x1 + 1, min(x + rw, w))
    y2 = max(y1 + 1, min(y + rh, h))
    return x1, y1, x2, y2


def crop_roi(frame: np.ndarray, roi: RectROI) -> np.ndarray:
    x1, y1, x2, y2 = clamp_roi(frame, roi)
    return frame[y1:y2, x1:x2].copy()


def interactive_select_named_roi(video_path: str, window_title: str, prompt: str) -> RectROI:
    cap = cv2.VideoCapture(video_path)
    ok, frame = cap.read()
    cap.release()

    if not ok or frame is None:
        raise RuntimeError("Impossibile leggere il primo frame del video.")

    display = frame.copy()
    scale = 1.0
    max_w = 1400
    h, w = display.shape[:2]

    if w > max_w:
        scale = max_w / w
        display = cv2.resize(
            display,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_AREA,
        )

    print(f"\n{prompt}\n")
    roi = cv2.selectROI(window_title, display, fromCenter=False, showCrosshair=True)
    cv2.destroyWindow(window_title)

    x, y, rw, rh = roi
    if rw == 0 or rh == 0:
        raise RuntimeError("ROI non selezionata.")

    x = int(round(x / scale))
    y = int(round(y / scale))
    rw = int(round(rw / scale))
    rh = int(round(rh / scale))

    return x, y, rw, rh


def order_quad_points(points: np.ndarray) -> QuadROI:
    pts = np.asarray(points, dtype=np.float32)
    if pts.shape != (4, 2):
        raise ValueError("Servono esattamente 4 punti.")

    center = np.mean(pts, axis=0)
    angles = np.arctan2(pts[:, 1] - center[1], pts[:, 0] - center[0])
    ordered = pts[np.argsort(angles)]

    sums = ordered[:, 0] + ordered[:, 1]
    start_idx = int(np.argmin(sums))
    ordered = np.roll(ordered, -start_idx, axis=0)

    area2 = 0.0
    for i in range(4):
        x1, y1 = ordered[i]
        x2, y2 = ordered[(i + 1) % 4]
        area2 += (x1 * y2) - (x2 * y1)
    if area2 < 0:
        ordered = np.array([ordered[0], ordered[3], ordered[2], ordered[1]], dtype=np.float32)

    return ordered.astype(np.float32)


def distance(p1: np.ndarray, p2: np.ndarray) -> float:
    return float(np.linalg.norm(p1 - p2))


def quad_output_size(quad: QuadROI) -> Tuple[int, int]:
    tl, tr, br, bl = quad
    width = int(round((distance(tl, tr) + distance(bl, br)) / 2.0))
    height = int(round((distance(tl, bl) + distance(tr, br)) / 2.0))
    width = max(50, width)
    height = max(50, height)
    return width, height


def clip_quad_to_frame(frame: np.ndarray, quad: QuadROI) -> QuadROI:
    h, w = frame.shape[:2]
    clipped = quad.copy().astype(np.float32)
    clipped[:, 0] = np.clip(clipped[:, 0], 0, w - 1)
    clipped[:, 1] = np.clip(clipped[:, 1], 0, h - 1)
    return clipped


def warp_quad_to_rect(frame: np.ndarray, quad: QuadROI) -> np.ndarray:
    quad = clip_quad_to_frame(frame, quad)
    out_w, out_h = quad_output_size(quad)

    dst = np.array([
        [0, 0],
        [out_w - 1, 0],
        [out_w - 1, out_h - 1],
        [0, out_h - 1],
    ], dtype=np.float32)

    matrix = cv2.getPerspectiveTransform(quad, dst)
    warped = cv2.warpPerspective(frame, matrix, (out_w, out_h))
    return warped


def build_quad_preview(display_frame: np.ndarray, points: List[Point]) -> np.ndarray:
    canvas = display_frame.copy()

    instructions = [
        "Click: 4 vertici slide | ENTER/SPACE conferma | R reset | ESC annulla",
        "Dopo il 4° punto viene mostrata una preview rettificata",
    ]
    y = 28
    for line in instructions:
        cv2.putText(canvas, line, (20, y), cv2.FONT_HERSHEY_SIMPLEX, 0.72, (0, 255, 255), 2, cv2.LINE_AA)
        y += 28

    for idx, pt in enumerate(points):
        cv2.circle(canvas, pt, 6, (0, 0, 255), -1, lineType=cv2.LINE_AA)
        cv2.putText(canvas, str(idx + 1), (pt[0] + 8, pt[1] - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2, cv2.LINE_AA)

    if len(points) >= 2:
        pts_arr = np.array(points, dtype=np.int32)
        cv2.polylines(canvas, [pts_arr], False, (0, 255, 0), 2, lineType=cv2.LINE_AA)

    if len(points) == 4:
        ordered = order_quad_points(np.array(points, dtype=np.float32))
        quad_int = ordered.astype(np.int32)
        cv2.polylines(canvas, [quad_int], True, (0, 255, 0), 2, lineType=cv2.LINE_AA)

        warped = warp_quad_to_rect(display_frame, ordered)
        preview_max_w = 420
        preview_max_h = 260
        wh, ww = warped.shape[:2]
        scale = min(preview_max_w / ww, preview_max_h / wh, 1.0)
        preview = cv2.resize(warped, (max(1, int(ww * scale)), max(1, int(wh * scale))), interpolation=cv2.INTER_AREA)

        px = max(10, canvas.shape[1] - preview.shape[1] - 15)
        py = max(80, 15)
        cv2.rectangle(canvas, (px - 3, py - 25), (px + preview.shape[1] + 3, py + preview.shape[0] + 3), (255, 255, 255), 2)
        cv2.putText(canvas, "Preview rettificata", (px, py - 6), cv2.FONT_HERSHEY_SIMPLEX, 0.65, (255, 255, 255), 2, cv2.LINE_AA)
        canvas[py:py + preview.shape[0], px:px + preview.shape[1]] = preview

    return canvas


def interactive_select_quad(video_path: str, window_title: str, prompt: str) -> QuadROI:
    cap = cv2.VideoCapture(video_path)
    ok, frame = cap.read()
    cap.release()

    if not ok or frame is None:
        raise RuntimeError("Impossibile leggere il primo frame del video.")

    base_frame = frame.copy()
    display_frame = base_frame.copy()
    scale = 1.0
    max_w = 1400
    h, w = display_frame.shape[:2]
    if w > max_w:
        scale = max_w / w
        display_frame = cv2.resize(
            display_frame,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_AREA,
        )

    points: List[Point] = []

    def on_mouse(event: int, x: int, y: int, flags: int, param: object) -> None:
        nonlocal points
        if event == cv2.EVENT_LBUTTONDOWN and len(points) < 4:
            points.append((x, y))

    print(f"\n{prompt}\n")
    print("Istruzioni: click su 4 vertici della slide. ENTER/SPACE conferma, R resetta, ESC annulla.")

    cv2.namedWindow(window_title, cv2.WINDOW_NORMAL)
    cv2.setMouseCallback(window_title, on_mouse)

    while True:
        canvas = build_quad_preview(display_frame, points)
        cv2.imshow(window_title, canvas)
        key = cv2.waitKey(20) & 0xFF

        if key in (13, 32):
            if len(points) == 4:
                break
        elif key in (ord('r'), ord('R')):
            points = []
        elif key == 27:
            cv2.destroyWindow(window_title)
            raise RuntimeError("Selezione quadrilatero annullata.")

    cv2.destroyWindow(window_title)

    pts = np.array(points, dtype=np.float32)
    pts /= scale
    pts = order_quad_points(pts)
    return pts.astype(np.float32)


def maybe_extract_candidate(
    cap: cv2.VideoCapture,
    frame_idx: int,
    fps: float,
    total_frames: int,
    trigger_roi: RectROI,
    compare_baseline: np.ndarray,
    step_frames: int,
    stabilization_samples: int,
    stabilization_ssim: float,
) -> Optional[Tuple[np.ndarray, float]]:
    """
    Cerca una versione stabile della nuova slide guardando qualche sample successivo.
    """
    best_candidate = None

    for k in range(1, stabilization_samples + 1):
        future_idx = min(frame_idx + k * step_frames, max(total_frames - 1, 0))
        cap.set(cv2.CAP_PROP_POS_FRAMES, future_idx)
        ok, future_frame = cap.read()
        if not ok or future_frame is None:
            break

        future_roi = crop_roi(future_frame, trigger_roi)
        metrics = compare_images(compare_baseline, future_roi)

        if metrics["ssim"] >= stabilization_ssim:
            best_candidate = (future_frame.copy(), future_idx / fps)
            break

    return best_candidate


def deduplicate_records(
    records: List[SlideCapture],
    dedup_ssim_threshold: float,
    dedup_mean_diff_threshold: float,
) -> List[SlideCapture]:
    if not records:
        return []

    deduped = [records[0]]

    for rec in records[1:]:
        prev = deduped[-1]
        metrics = compare_images(prev.image, rec.image)

        is_duplicate = (
            metrics["ssim"] >= dedup_ssim_threshold
            or metrics["mean_diff"] <= dedup_mean_diff_threshold
        )

        if is_duplicate:
            try:
                os.remove(rec.filename)
            except OSError:
                pass
        else:
            deduped.append(rec)

    return deduped


def renumber_files(records: List[SlideCapture]) -> List[SlideCapture]:
    updated = []

    for new_idx, rec in enumerate(records, start=1):
        dirname = os.path.dirname(rec.filename)
        new_name = os.path.join(
            dirname,
            f"slide_{new_idx:03d}_{format_timestamp(rec.time_sec)}.png"
        )

        if os.path.abspath(new_name) != os.path.abspath(rec.filename):
            if os.path.exists(new_name):
                os.remove(new_name)
            os.rename(rec.filename, new_name)

        updated.append(
            SlideCapture(
                index=new_idx,
                time_sec=rec.time_sec,
                filename=new_name,
                image=rec.image,
            )
        )

    return updated


def extract_slides(
    video_path: str,
    output_dir: Optional[str],
    sample_every_sec: float,
    ssim_threshold: float,
    mean_diff_threshold: float,
    changed_ratio_threshold: float,
    min_slide_duration_sec: float,
    stabilization_samples: int,
    stabilization_ssim: float,
    save_mode: str,
    dedup_ssim_threshold: float,
    dedup_mean_diff_threshold: float,
    save_first_slide: bool,
    use_separate_trigger_roi: bool,
) -> None:
    if output_dir is None:
        output_dir = default_output_dir_from_video(video_path)

    ensure_dir(output_dir)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Impossibile aprire il video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps <= 0 or math.isnan(fps):
        fps = 25.0

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = total_frames / fps if total_frames > 0 else 0.0

    print(f"FPS: {fps:.3f}")
    print(f"Frame totali: {total_frames}")
    print(f"Durata stimata: {duration_sec:.1f} s")
    print(f"Output directory: {output_dir}")

    slide_quad = interactive_select_quad(
        video_path,
        "Seleziona slide (4 vertici)",
        "Seleziona i 4 vertici della slide da salvare. Dopo il 4° click vedrai la preview rettificata. Premi INVIO o SPAZIO per confermare."
    )
    print("Slide quad ordinato (TL, TR, BR, BL):")
    for i, (x, y) in enumerate(slide_quad, start=1):
        print(f"  P{i}: x={x:.1f}, y={y:.1f}")

    if use_separate_trigger_roi:
        trigger_roi = interactive_select_named_roi(
            video_path,
            "Seleziona area trigger",
            "Seleziona l'area rettangolare da usare per rilevare il cambio slide, poi premi INVIO o SPAZIO."
        )
    else:
        xs = slide_quad[:, 0]
        ys = slide_quad[:, 1]
        x1 = int(np.floor(np.min(xs)))
        y1 = int(np.floor(np.min(ys)))
        x2 = int(np.ceil(np.max(xs)))
        y2 = int(np.ceil(np.max(ys)))
        trigger_roi = (x1, y1, max(1, x2 - x1), max(1, y2 - y1))
        print("Trigger ROI non separata: uso il bounding box rettangolare della quadrilatera slide.")

    print(f"ROI trigger: x={trigger_roi[0]}, y={trigger_roi[1]}, w={trigger_roi[2]}, h={trigger_roi[3]}")

    step_frames = max(1, int(round(sample_every_sec * fps)))
    min_gap_samples = max(1, int(math.ceil(min_slide_duration_sec / sample_every_sec)))

    records: List[SlideCapture] = []

    prev_saved_roi = None
    last_saved_sample_idx = -10_000
    slide_index = 1

    sample_idx = 0
    frame_idx = 0

    while True:
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            break

        current_warped_slide = warp_quad_to_rect(frame, slide_quad)
        current_trigger_roi = crop_roi(frame, trigger_roi)
        time_sec = frame_idx / fps

        if prev_saved_roi is None:
            if save_first_slide:
                img_to_save = current_warped_slide if save_mode == "crop" else frame
                filename = save_image(img_to_save, output_dir, slide_index, time_sec)
                records.append(SlideCapture(slide_index, time_sec, filename, current_warped_slide))
                print(f"[SALVATA] Slide {slide_index} @ {time_sec:.1f}s -> {filename}")
                slide_index += 1

            prev_saved_roi = current_trigger_roi.copy()
            last_saved_sample_idx = sample_idx
        else:
            metrics = compare_images(prev_saved_roi, current_trigger_roi)
            enough_time_passed = (sample_idx - last_saved_sample_idx) >= min_gap_samples

            if enough_time_passed and is_slide_change(
                metrics,
                ssim_threshold=ssim_threshold,
                mean_diff_threshold=mean_diff_threshold,
                changed_ratio_threshold=changed_ratio_threshold,
            ):
                candidate = maybe_extract_candidate(
                    cap=cap,
                    frame_idx=frame_idx,
                    fps=fps,
                    total_frames=total_frames,
                    trigger_roi=trigger_roi,
                    compare_baseline=current_trigger_roi,
                    step_frames=step_frames,
                    stabilization_samples=stabilization_samples,
                    stabilization_ssim=stabilization_ssim,
                )

                if candidate is not None:
                    stable_frame, stable_time = candidate
                    stable_warped_slide = warp_quad_to_rect(stable_frame, slide_quad)
                    stable_trigger_roi = crop_roi(stable_frame, trigger_roi)

                    img_to_save = stable_warped_slide if save_mode == "crop" else stable_frame
                    filename = save_image(img_to_save, output_dir, slide_index, stable_time)
                    records.append(SlideCapture(slide_index, stable_time, filename, stable_warped_slide))

                    print(
                        f"[SALVATA] Slide {slide_index} @ {stable_time:.1f}s "
                        f"(ssim={metrics['ssim']:.3f}, mean_diff={metrics['mean_diff']:.2f}, "
                        f"changed_ratio={metrics['changed_ratio']:.3f}) -> {filename}"
                    )

                    prev_saved_roi = stable_trigger_roi.copy()
                    last_saved_sample_idx = sample_idx
                    slide_index += 1

        frame_idx += step_frames
        sample_idx += 1

    cap.release()

    print("\nDeduplicazione finale...")
    records = deduplicate_records(
        records,
        dedup_ssim_threshold=dedup_ssim_threshold,
        dedup_mean_diff_threshold=dedup_mean_diff_threshold,
    )
    records = renumber_files(records)

    csv_path = os.path.join(output_dir, "slides.csv")
    write_csv(records, csv_path)

    print("\nFatto.")
    print(f"Slide finali: {len(records)}")
    print(f"Cartella output: {output_dir}")
    print(f"CSV: {csv_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estrae le slide da un video quando cambia il contenuto dell'area trigger; la slide viene selezionata come quadrilatero e raddrizzata prima del salvataggio."
    )

    parser.add_argument("video", help="Percorso del file video")
    parser.add_argument(
        "-o", "--output",
        default=None,
        help='Cartella di output (default: "<nome video> slides")'
    )
    parser.add_argument(
        "--sample-every",
        type=float,
        default=1.0,
        help="Campiona un frame ogni N secondi (default: 1.0)"
    )
    parser.add_argument(
        "--ssim-threshold",
        type=float,
        default=0.92,
        help="Soglia SSIM per rilevamento cambio slide (default: 0.92)"
    )
    parser.add_argument(
        "--mean-diff-threshold",
        type=float,
        default=8.0,
        help="Soglia mean diff per rilevamento cambio slide (default: 8.0)"
    )
    parser.add_argument(
        "--changed-ratio-threshold",
        type=float,
        default=0.06,
        help="Soglia percentuale pixel cambiati (0-1) (default: 0.06)"
    )
    parser.add_argument(
        "--min-slide-duration",
        type=float,
        default=2.0,
        help="Tempo minimo tra due slide salvate (default: 2.0)"
    )
    parser.add_argument(
        "--stabilization-samples",
        type=int,
        default=2,
        help="Quanti campioni successivi controllare per stabilizzare la slide (default: 2)"
    )
    parser.add_argument(
        "--stabilization-ssim",
        type=float,
        default=0.97,
        help="Quanto dev'essere stabile la nuova slide rispetto al campione dopo (default: 0.97)"
    )
    parser.add_argument(
        "--save-mode",
        choices=["crop", "full"],
        default="crop",
        help="Salva la slide rettificata oppure il frame intero (default: crop)"
    )
    parser.add_argument(
        "--dedup-ssim-threshold",
        type=float,
        default=0.985,
        help="Soglia SSIM per deduplicazione finale (default: 0.985)"
    )
    parser.add_argument(
        "--dedup-mean-diff-threshold",
        type=float,
        default=2.0,
        help="Soglia mean diff per deduplicazione finale (default: 2.0)"
    )
    parser.add_argument(
        "--no-first-slide",
        action="store_true",
        help="Non salvare subito la prima slide"
    )
    parser.add_argument(
        "--separate-trigger-roi",
        action="store_true",
        help="Permette di selezionare una ROI trigger rettangolare separata; altrimenti usa il bounding box della quadrilatera slide"
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    extract_slides(
        video_path=args.video,
        output_dir=args.output,
        sample_every_sec=args.sample_every,
        ssim_threshold=args.ssim_threshold,
        mean_diff_threshold=args.mean_diff_threshold,
        changed_ratio_threshold=args.changed_ratio_threshold,
        min_slide_duration_sec=args.min_slide_duration,
        stabilization_samples=args.stabilization_samples,
        stabilization_ssim=args.stabilization_ssim,
        save_mode=args.save_mode,
        dedup_ssim_threshold=args.dedup_ssim_threshold,
        dedup_mean_diff_threshold=args.dedup_mean_diff_threshold,
        save_first_slide=not args.no_first_slide,
        use_separate_trigger_roi=args.separate_trigger_roi,
    )


if __name__ == "__main__":
    main()
