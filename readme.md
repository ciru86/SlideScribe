# Slidescribe v2

A reproducible pipeline to convert lecture videos into structured, slide-aligned documents (PDF/DOCX) using deterministic preprocessing and LLM-assisted text normalization.

---

## Features

* End-to-end pipeline: video → slides → subtitles → structured text → PDF/DOCX
* Deterministic preprocessing (ffmpeg / yt-dlp)
* Chunked SRT processing for LLM stability
* Slide–text alignment via per-slide segmentation
* Reproducible environment via local `.venv`
* Script-first design (no global Python state required)

---

## Requirements

### System

* macOS / Linux (tested on macOS arm64)
* `bash` (>= 4)
* `ffmpeg`
* `yt-dlp`

Install via Homebrew (macOS):

```bash
brew install ffmpeg yt-dlp
```

### Python

* Python >= 3.10

All Python dependencies are pinned in `requirements.txt`.

---

## Setup

### 1. Python environment

Create the project virtual environment and install dependencies:

```bash
./create_venv.sh
```

This will:

* create `.venv/`
* install `requirements.txt`
* ensure a self-contained runtime

> Do **not** rely on system Python.

All Python scripts must be executed via:

```bash
./.venv/bin/python
```

---

### 2. ChatGPT Wrapper

The orchestrator depends on a local `chatgpt` CLI wrapper to communicate with the API.

#### Install binary

```bash
mv chatgpt ~/.local/bin/
chmod +x ~/.local/bin/chatgpt
```

#### Configure API key

```bash
nano ~/.secrets
```

```bash
export OPENAI_API_KEY={{YOUR_OPENAI_API_KEY}}
```

#### Update shell configuration (`~/.zshrc`)

```bash
# User bin
export PATH="$PATH:$HOME/.local/bin"

# Secrets
[ -f ~/.secrets ] && source ~/.secrets
```

Apply changes:

```bash
source ~/.zshrc
```

#### Verify

```bash
which chatgpt
chatgpt --help
```

The pipeline will fail if the wrapper is not available in `$PATH` or if the API key is missing.

---

## Usage

Run the orchestrator:

```bash
./Slidescribe_v2.sh
```

Interactive parameters:

| Parameter      | Description                     |
| -------------- | ------------------------------- |
| WORKDIR        | Output working directory        |
| VIDEO_URL      | YouTube source                  |
| VIDEO_BASENAME | Base name for all artifacts     |
| PROMPT         | LLM correction prompt           |
| ROI_MODE       | Optional slide region selection |

---

## Subtitles Language

`yt-dlp` is currently invoked with:

```bash
--sub-langs "it,it-IT,it.*,ita"
```

To change subtitle language, modify this flag directly inside `Slidescribe_v2.sh`.

Examples:

* English:

```bash
--sub-langs "en,en-US,en.*,eng"
```

* Multi-language fallback:

```bash
--sub-langs "en,it,en.*,it.*"
```

No runtime flag is currently exposed: this is a **hardcoded configuration**.

---

-------|------------| | WORKDIR | Output working directory | | VIDEO_URL | YouTube source | | VIDEO_BASENAME | Base name for all artifacts | | PROMPT | LLM correction prompt | | ROI_MODE | Optional slide region selection |

---

## Architecture

The pipeline is intentionally split into deterministic and probabilistic stages.

### Deterministic stages

* Video download (`yt-dlp`)
* Audio extraction (`ffmpeg`)
* Subtitle retrieval (auto-subs)
* Frame sampling / slide extraction
* Slide indexing (`slides.csv`)

### Probabilistic stage (LLM)

* Text normalization
* Error correction (ASR noise)
* Readability improvement (non-semantic)

LLM usage is **strictly bounded**:

* no content generation
* no hallucination tolerance
* timestamp preservation enforced

---

## Pipeline

```text
Video
  ↓
yt-dlp
  ↓
Raw video + subtitles (.srt)
  ↓
ffmpeg (frames)
  ↓
Slide extraction → slides.csv
  ↓
SRT chunking
  ↓
LLM correction (per chunk)
  ↓
Recomposition
  ↓
Slide-text alignment
  ↓
PDF / DOCX rendering
```

---

## Data Model

### slides.csv

| Column     | Description     |
| ---------- | --------------- |
| slide_id   | incremental id  |
| timestamp  | frame timestamp |
| image_path | slide image     |

### SRT (post-processing)

* normalized block duration
* chunk-safe segmentation
* LLM-corrected text

---

## Design Choices

### Chunking strategy

Large transcripts are split into manageable chunks to:

* reduce token overflow risk
* increase determinism
* allow partial retries

### No full-SRT LLM pass

Avoids:

* formatting corruption
* timestamp drift
* catastrophic failures on long inputs

### Explicit interpreter usage

Avoids environment leakage and ensures reproducibility.

---

## Error Handling

* `set -euo pipefail` enforced in shell scripts
* early exit on missing dependencies
* intermediate artifacts preserved for debugging

Recommended checks:

```bash
shellcheck Slidescribe_v2.sh
```

---

## Performance Considerations

* Frame extraction is CPU-bound
* LLM stage is latency-bound
* Chunk size is the main trade-off parameter

---

## Limitations

* Dependent on subtitle quality (ASR errors propagate)
* Slide detection assumes stable layouts
* No semantic restructuring (by design)

---

## Roadmap

* Parallel chunk processing
* Deterministic slide detection improvements
* Optional GUI (SwiftUI frontend)
* Config file (YAML/JSON) instead of interactive input

---

## Contributing

PRs should:

* preserve deterministic stages
* avoid increasing LLM surface area
* maintain reproducibility

---

## License

This project is licensed under the **MIT License**.

You are free to:

* use
* modify
* distribute
* sublicense
* use commercially

Provided that the original copyright and license notice are included.

---

### MIT License

Copyright (c) 2026 Pier Paolo Cirulli

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
