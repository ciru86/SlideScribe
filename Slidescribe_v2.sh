#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CONFIG
# ============================================================
SCREENSHOT_SCRIPT="Screenshot_grabber.py"
EXPORT_SCRIPT="export_for_llm.py"
IMPORT_SCRIPT="import_corrected_for_pdf_docx.py"
PDF_SCRIPT="slides_and_texts_to_pdf.py"

MODEL="gpt-5.4"
TEMPERATURE="0.5"
MAX_OUTPUT_TOKENS="70000"
VERBOSITY="low"
CHUNK_SIZE="20"
USE_SAFARI_COOKIES=0
VERBOSE_LOGS=0
YTDLP_FALLBACK_PATH="$HOME/.local/bin/yt-dlp"

# ============================================================
# PATH PROGETTO / PYTHON DEL VENV
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_PYTHON="${SCRIPT_DIR}/.venv/bin/python"

# ============================================================
# UTILITY
# ============================================================
die() {
  echo "Errore: $*" >&2
  exit 1
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Comando non trovato: $1"
}

require_file() {
  [ -f "$1" ] || die "File non trovato: $1"
}

require_dir() {
  [ -d "$1" ] || die "Cartella non trovata: $1"
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

file_exists_nonempty() {
  [ -f "$1" ] && [ -s "$1" ]
}

timestamp_for_backup() {
  date '+%Y%m%d-%H%M%S'
}

backup_file_if_exists() {
  local path="$1"
  local reason="${2:-backup}"
  local backup=""

  if [ -f "$path" ]; then
    backup="${path}.${reason}.$(timestamp_for_backup)"
    mv "$path" "$backup"
    log "Backup creato: $backup"
  fi
}

prompt_nonempty() {
  local label="$1"
  local value=""
  while true; do
    printf "%s" "$label" > /dev/tty
    IFS= read -r value < /dev/tty
    value="$(trim "$value")"
    [ -n "$value" ] && break
    echo "Valore vuoto. Riprova." > /dev/tty
  done
  printf '%s' "$value"
}

prompt_choice_12() {
  local value=""
  while true; do
    printf "Seleziona modalità ROI (1 = Slide Area e Trigger Area uguali e rettangolari, 2 = Slide Area ≠ Trigger Area e Slide Area poligonale ): " > /dev/tty
    IFS= read -r value < /dev/tty
    case "$value" in
      1|2)
        printf '%s' "$value"
        return 0
        ;;
      *)
        echo "Devi digitare 1 oppure 2." > /dev/tty
        ;;
    esac
  done
}

prompt_optional() {
  local message="$1"
  local value
  printf "%s " "$message" > /dev/tty
  IFS= read -r value < /dev/tty
  value="$(trim "$value")"
  printf '%s
' "$value"
}

build_llm_prompt() {
  local lesson_topic="$1"
  local terminology_context="$2"
  local intro_line=""
  local terminology_section=""

  if [[ -n "$lesson_topic" ]]; then
    intro_line="Ti fornisco un file TXT strutturato in chunk di slide. È la sbobinatura di una lezione. **${lesson_topic}**. È stata ottenuta da trascrizione automatica (Whisper / speech-to-text), quindi può contenere errori di trascrizione, punteggiatura imperfetta, termini tecnici sbagliati e frasi poco naturali."
  else
    intro_line="Ti fornisco un file TXT strutturato in chunk di slide. È la sbobinatura di una lezione ottenuta da trascrizione automatica (Whisper / speech-to-text), quindi può contenere errori di trascrizione, punteggiatura imperfetta, termini tecnici sbagliati e frasi poco naturali."
  fi

  if [[ -n "$terminology_context" ]]; then
    terminology_section=$(cat <<EOF

Contesto terminologico:
**${terminology_context}**
EOF
)
  fi

  cat <<EOF
${intro_line}

Il tuo compito è ripulire il testo associato a ciascuna slide, migliorandone la leggibilità, ma senza aggiungere contenuti non presenti nell’originale.

Regole obbligatorie:
- NON modificare i delimitatori di chunk
- NON modificare le intestazioni del tipo `----- BEGIN SLIDE 0001 -----`
- NON modificare le intestazioni del tipo `----- END SLIDE 0001 -----`
- NON modificare la riga `TEXT:`
- NON aggiungere o rimuovere slide
- NON rinumerare le slide
- NON aggiungere commenti, note, introduzioni o conclusioni
- NON aggiungere concetti, spiegazioni, esempi o dettagli che non siano già presenti nel testo
- NON completare arbitrariamente frasi tronche se il contenuto mancante non è chiaramente ricostruibile dal testo stesso
- NON raccordare artificialmente una slide con la successiva o con la precedente
- restituisci solo il file corretto, mantenendo identica la struttura di input

Puoi modificare SOLO il testo sotto `TEXT:` per:
- correggere errori di trascrizione
- correggere termini tecnici
- migliorare punteggiatura e leggibilità
- riformulare frasi poco naturali o troppo orali in un italiano più chiaro e scorrevole
- eliminare ripetizioni inutili o piccoli inciampi tipici del parlato, purché il significato resti invariato${terminology_section}

Importante:
- il chunk può iniziare o finire nel mezzo di un discorso
- alcune frasi possono risultare incomplete perché continuano nella slide successiva o nel chunk successivo
- in questi casi migliora il testo solo se possibile senza inventare il contenuto mancante
- se una correzione non è sicura, mantieni una versione prudente e vicina all’originale

Obiettivo finale:
produrre un testo più leggibile, ma semanticamente fedele all’originale.

Restituisci esclusivamente il testo corretto nello stesso identico formato ricevuto.
EOF
}

validate_youtube_url() {
  local url="$1"
  local validation_log
  validation_log="${TMPDIR:-/tmp}/slidescribe_validate_ytdlp.$$ .stderr.log"
  validation_log="${validation_log// /}"
  run_ytdlp_with_fallback "$validation_log" "validazione URL" --simulate --skip-download -- "$url" >/dev/null
  rm -f "$validation_log"
}

extract_json_field_with_python() {
  local json_input="$1"
  local field="$2"

  JSON_INPUT="$json_input" python3 - "$field" <<'PY'
import json, os, sys

field = sys.argv[1]
raw = os.environ["JSON_INPUT"]

try:
    data = json.loads(raw)
except Exception:
    sys.exit(2)

value = data.get(field, "")
if isinstance(value, str):
    print(value)
PY
}

count_slides_from_csv() {
  local csv_path="$1"

  "$VENV_PYTHON" - "$csv_path" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
with path.open("r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    rows = list(reader)
print(len(rows))
PY
}

cleanup_intermediate_srts() {
  local path

  for path in "$IT_SRT" "$IT_ORIG_SRT"; do
    if [ -f "$path" ]; then
      rm -f "$path"
      log "Rimosso SRT intermedio: $path"
    fi
  done
}

move_final_outputs_to_workdir() {
  require_file "$FINAL_PDF_IN_SLIDES"
  require_file "$FINAL_DOCX_IN_SLIDES"

  if [ -f "$FINAL_PDF" ]; then
    backup_file_if_exists "$FINAL_PDF" "pre-move"
  fi
  if [ -f "$FINAL_DOCX" ]; then
    backup_file_if_exists "$FINAL_DOCX" "pre-move"
  fi

  mv -f "$FINAL_PDF_IN_SLIDES" "$FINAL_PDF"
  mv -f "$FINAL_DOCX_IN_SLIDES" "$FINAL_DOCX"

  log "PDF spostato in: $FINAL_PDF"
  log "DOCX spostato in: $FINAL_DOCX"
}


usage() {
  cat <<'EOF'
Uso:
  Slidescribe_v2.sh [opzioni]

Opzioni:
  --cookie           Usa yt-dlp con --cookies-from-browser Safari
  -v, --verbose      Mostra a schermo anche i messaggi che vengono salvati in $LOG_DIR
  -h, --help         Mostra questo aiuto

Nota yt-dlp fallback:
  Se yt-dlp fallisce, lo script prova automaticamente ~/.local/bin/yt-dlp
  aggiungendo anche:
    --cookies-from-browser Safari
    --impersonate Safari-26.0:Macos-26
EOF
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --cookie)
        USE_SAFARI_COOKIES=1
        ;;
      -v|--verbose)
        VERBOSE_LOGS=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Opzione non riconosciuta: $1"
        ;;
    esac
    shift
  done
}

build_ytdlp_base_cmd() {
  local -a cmd
  cmd=(yt-dlp --no-playlist)

  if [ "$USE_SAFARI_COOKIES" -eq 1 ]; then
    cmd+=(--cookies-from-browser Safari)
  fi

  printf '%s
' "${cmd[@]}"
}

build_ytdlp_fallback_cmd() {
  local -a cmd
  cmd=("$YTDLP_FALLBACK_PATH" --no-playlist --cookies-from-browser Safari --impersonate "Safari-26.0:Macos-26")
  printf '%s
' "${cmd[@]}"
}

run_logged_command() {
  local stdout_log="$1"
  local stderr_log="$2"
  local show_on_screen="${3:-0}"
  shift 3

  if [ "$show_on_screen" = "1" ] || [ "$show_on_screen" = "always" ] || [ "$VERBOSE_LOGS" -eq 1 ]; then
    "$@"       > >(tee "$stdout_log")       2> >(tee "$stderr_log" >&2)
  else
    "$@" >"$stdout_log" 2>"$stderr_log"
  fi
}

run_ytdlp_with_fallback() {
  local stderr_log="$1"
  local action_label="$2"
  shift 2

  local -a base_cmd
  local -a fallback_cmd
  local rc=0

  base_cmd=(yt-dlp --no-playlist)
  if [ "$USE_SAFARI_COOKIES" -eq 1 ]; then
    base_cmd+=(--cookies-from-browser Safari)
  fi

  fallback_cmd=("$YTDLP_FALLBACK_PATH" --no-playlist --cookies-from-browser Safari --impersonate "Safari-26.0:Macos-26")

  if [ "$VERBOSE_LOGS" -eq 1 ]; then
    log "yt-dlp primario (${action_label}): ${base_cmd[*]} $*"
  fi

  set +e
  "${base_cmd[@]}" "$@" 2> >(tee -a "$stderr_log" >&2)
  rc=$?
  set -e

  if [ $rc -eq 0 ]; then
    return 0
  fi

  log "yt-dlp primario fallito per ${action_label} (exit ${rc}). Provo fallback: $YTDLP_FALLBACK_PATH"

  if [ ! -x "$YTDLP_FALLBACK_PATH" ]; then
    die "yt-dlp primario fallito e fallback non disponibile/eseguibile: $YTDLP_FALLBACK_PATH"
  fi

  if [ "$VERBOSE_LOGS" -eq 1 ]; then
    log "yt-dlp fallback (${action_label}): ${fallback_cmd[*]} $*"
  fi

  set +e
  "${fallback_cmd[@]}" "$@" 2> >(tee -a "$stderr_log" >&2)
  rc=$?
  set -e

  if [ $rc -ne 0 ]; then
    die "yt-dlp fallito anche col fallback per ${action_label} (exit ${rc}). Vedi log: $stderr_log"
  fi
}
run_chatgpt_upload() {
  local chunk_file="$1"

  if [ "$VERBOSE_LOGS" -eq 1 ]; then
    chatgpt --upload-file "$chunk_file" 2> >(tee -a "$CHATGPT_UPLOAD_STDERR" >&2)
  else
    chatgpt --upload-file "$chunk_file" 2>>"$CHATGPT_UPLOAD_STDERR"
  fi
}

# ============================================================
# CHECK PRELIMINARI
# ============================================================
parse_args "$@"

require_command python3
require_command yt-dlp
require_command ffmpeg
require_command chatgpt

if [ -x "$YTDLP_FALLBACK_PATH" ]; then
  log "Fallback yt-dlp disponibile: $YTDLP_FALLBACK_PATH"
else
  log "Fallback yt-dlp non trovato o non eseguibile: $YTDLP_FALLBACK_PATH"
fi

[ -x "$VENV_PYTHON" ] || die "Virtualenv non trovato o non valido: $VENV_PYTHON"
require_file "${SCRIPT_DIR}/${SCREENSHOT_SCRIPT}"
require_file "${SCRIPT_DIR}/${EXPORT_SCRIPT}"
require_file "${SCRIPT_DIR}/${IMPORT_SCRIPT}"
require_file "${SCRIPT_DIR}/${PDF_SCRIPT}"

# ============================================================
# INPUT UTENTE
# ============================================================
WORKDIR_INPUT="$(prompt_nonempty "Inserisci il path della cartella di lavoro: ")"
mkdir -p "$WORKDIR_INPUT"
WORKDIR="$(cd "$WORKDIR_INPUT" && pwd)"
log "Cartella di lavoro: $WORKDIR"

YOUTUBE_URL="$(prompt_nonempty "Inserisci URL video YouTube: ")"
validate_youtube_url "$YOUTUBE_URL" || die "URL YouTube non valido o non accessibile con yt-dlp"
log "URL verificato"
[ "$USE_SAFARI_COOKIES" -eq 1 ] && log "Cookie Safari abilitati per yt-dlp"
[ "$VERBOSE_LOGS" -eq 1 ] && log "Modalità verbose abilitata: i log verranno mostrati anche a schermo"

VIDEO_BASENAME="$(prompt_nonempty "Come vuoi chiamare il video finale (senza estensione)? ")"
LESSON_TOPIC="$(prompt_optional "Inserisci l'argomento della lezione (lascia vuoto per usare la versione generica del prompt):")"
TERMINOLOGY_CONTEXT="$(prompt_optional "Inserisci il contesto terminologico, se utile (lascia vuoto per omettere l'intera sezione):")"
PROMPT_TEXT="$(build_llm_prompt "$LESSON_TOPIC" "$TERMINOLOGY_CONTEXT")"

# ============================================================
# PATH PRINCIPALI
# ============================================================
VIDEO_PATH="${WORKDIR}/${VIDEO_BASENAME}.mkv"
ORIGINAL_SRT="${WORKDIR}/${VIDEO_BASENAME}.original.srt"
IT_SRT="${WORKDIR}/${VIDEO_BASENAME}.it.srt"
IT_ORIG_SRT="${WORKDIR}/${VIDEO_BASENAME}.it-orig.srt"

SLIDES_DIR="${WORKDIR}/${VIDEO_BASENAME} slides"

FINAL_PDF_IN_SLIDES="${SLIDES_DIR}/${VIDEO_BASENAME}.pdf"
FINAL_DOCX_IN_SLIDES="${SLIDES_DIR}/${VIDEO_BASENAME}.docx"
FINAL_PDF="${WORKDIR}/${VIDEO_BASENAME}.pdf"
FINAL_DOCX="${WORKDIR}/${VIDEO_BASENAME}.docx"

SLIDES_DIR="${WORKDIR}/${VIDEO_BASENAME} slides"

LLM_CHUNKS_DIR="${WORKDIR}/llm_chunks"
LLM_CORRECTED_DIR="${WORKDIR}/llm_corrected"
LLM_MERGED_DIR="${WORKDIR}/llm_merged"
MERGED_SLIDE_TEXTS_JSON="${LLM_MERGED_DIR}/${VIDEO_BASENAME}.slide_texts.json"

LOG_DIR="${WORKDIR}/logs"

mkdir -p "$LOG_DIR" "$LLM_CHUNKS_DIR" "$LLM_CORRECTED_DIR" "$LLM_MERGED_DIR"

SCREENSHOT_STDOUT="${LOG_DIR}/screenshot.stdout.log"
SCREENSHOT_STDERR="${LOG_DIR}/screenshot.stderr.log"

EXPORT_STDOUT="${LOG_DIR}/export.stdout.log"
EXPORT_STDERR="${LOG_DIR}/export.stderr.log"

CHATGPT_UPLOAD_STDERR="${LOG_DIR}/chatgpt_upload.stderr.log"
CHATGPT_RUN_STDERR="${LOG_DIR}/chatgpt_run.stderr.log"

IMPORT_STDOUT="${LOG_DIR}/import.stdout.log"
IMPORT_STDERR="${LOG_DIR}/import.stderr.log"

PDF_STDOUT="${LOG_DIR}/pdf.stdout.log"
PDF_STDERR="${LOG_DIR}/pdf.stderr.log"

# ============================================================
# STEP 1: VIDEO
# ============================================================
if file_exists_nonempty "$VIDEO_PATH"; then
  log "Video già presente, skip download: $VIDEO_PATH"
else
  log "Download video in massima qualità + remux MKV..."
  yt-dlp \
    --no-playlist \
    -f "bv*+ba/b" \
    --remux-video mkv \
    -o "${WORKDIR}/${VIDEO_BASENAME}.%(ext)s" \
    -- "$YOUTUBE_URL"

  require_file "$VIDEO_PATH"
  log "Video pronto: $VIDEO_PATH"
fi

# ============================================================
# STEP 2: SOTTOTITOLI ORIGINALI
# ============================================================
if file_exists_nonempty "$ORIGINAL_SRT"; then
  log "SRT originale già presente, skip download: $ORIGINAL_SRT"
else
  log "Download sottotitoli automatici italiani + conversione SRT..."
  yt-dlp \
    --no-playlist \
    --skip-download \
    --write-auto-subs \
    --sub-langs "it,it-IT,it.*,ita" \
    --convert-subs srt \
    -o "${WORKDIR}/${VIDEO_BASENAME}.%(ext)s" \
    -- "$YOUTUBE_URL"

  DOWNLOADED_SRT="$(
    find "$WORKDIR" -maxdepth 1 -type f \
      \( -name "${VIDEO_BASENAME}*.it*.srt" -o -name "${VIDEO_BASENAME}*.srt" \) \
      | sort | head -n 1
  )"

  [ -n "${DOWNLOADED_SRT:-}" ] || die "Nessun file SRT trovato dopo il download dei sottotitoli"

  cp -f "$DOWNLOADED_SRT" "$ORIGINAL_SRT"
  require_file "$ORIGINAL_SRT"
  log "SRT originale pronto: $ORIGINAL_SRT"
fi

# ============================================================
# CHECKPOINT / RESUME
# ============================================================
screenshots_needed=1
llm_pipeline_needed=1

if [ -d "$SLIDES_DIR" ] && [ -f "${SLIDES_DIR}/slides.csv" ] && [ -s "${SLIDES_DIR}/slides.csv" ]; then
  log "Slide e slides.csv già presenti, skip Screenshot_grabber: $SLIDES_DIR"
  screenshots_needed=0
else
  log "Cartella slide non trovata o incompleta: Screenshot_grabber verrà eseguito"
fi

if file_exists_nonempty "$MERGED_SLIDE_TEXTS_JSON"; then
  log "Slide texts JSON già presente, skip pipeline LLM: $MERGED_SLIDE_TEXTS_JSON"
  llm_pipeline_needed=0
else
  log "Slide texts JSON da generare"
fi

if [ "$screenshots_needed" -eq 1 ]; then
  ROI_MODE="$(prompt_choice_12)"
else
  ROI_MODE=""
fi

# ============================================================
# TASK 1: SCREENSHOT GRABBER
# ============================================================
task_screenshots() {
  local -a cmd
  cmd=(
    "$VENV_PYTHON"
    "${SCRIPT_DIR}/${SCREENSHOT_SCRIPT}"
    "$VIDEO_PATH"
    -o "$SLIDES_DIR"
  )

  if [ "$ROI_MODE" = "2" ]; then
    cmd+=(--separate-trigger-roi)
  fi

  log "Avvio Screenshot_grabber..."
  log "Stdout: $SCREENSHOT_STDOUT"
  log "Stderr: $SCREENSHOT_STDERR"
  run_logged_command "$SCREENSHOT_STDOUT" "$SCREENSHOT_STDERR" 1 "${cmd[@]}"

  [ -d "$SLIDES_DIR" ] || die "Screenshot_grabber terminato ma cartella slide non trovata: $SLIDES_DIR"
  [ -f "${SLIDES_DIR}/slides.csv" ] || die "Screenshot_grabber terminato ma slides.csv non trovato in: $SLIDES_DIR"
  [ -s "${SLIDES_DIR}/slides.csv" ] || die "Screenshot_grabber terminato ma slides.csv è vuoto: ${SLIDES_DIR}/slides.csv"
}

# ============================================================
# TASK 2: EXPORT + CHATGPT CHUNKS + IMPORT
# ============================================================
run_llm_pipeline() {
  local slides_csv="${SLIDES_DIR}/slides.csv"
  local expected_slides
  local chunk_file
  local chunk_basename
  local corrected_file
  local upload_json
  local file_id
  local raw_json_path

  require_file "$slides_csv"
  require_file "$ORIGINAL_SRT"

  log "Avvio export_for_llm.py..."
  "$VENV_PYTHON" "${SCRIPT_DIR}/${EXPORT_SCRIPT}" \
    --srt "$ORIGINAL_SRT" \
    --slides-csv "$slides_csv" \
    --output-dir "$LLM_CHUNKS_DIR" \
    --base-name "$VIDEO_BASENAME" \
    --chunk-size "$CHUNK_SIZE" \
    >"$EXPORT_STDOUT" 2>"$EXPORT_STDERR"

  expected_slides="$(count_slides_from_csv "$slides_csv")"
  [ -n "$expected_slides" ] || die "Impossibile contare le slide da $slides_csv"
  log "Slide attese dal CSV: $expected_slides"

  shopt -s nullglob
  local chunk_files=( "$LLM_CHUNKS_DIR"/"${VIDEO_BASENAME}".chunk_*.txt )
  shopt -u nullglob

  [ "${#chunk_files[@]}" -gt 0 ] || die "Nessun chunk generato in $LLM_CHUNKS_DIR"

  for chunk_file in "${chunk_files[@]}"; do
    chunk_basename="$(basename "$chunk_file" .txt)"
    corrected_file="${LLM_CORRECTED_DIR}/${chunk_basename}.corrected.txt"
    raw_json_path="${LLM_CORRECTED_DIR}/${chunk_basename}.raw.json"

    if file_exists_nonempty "$corrected_file"; then
      log "Chunk corretto già presente, skip: $corrected_file"
      continue
    fi

    log "Upload chunk a ChatGPT: $chunk_file"
    upload_json="$(run_chatgpt_upload "$chunk_file")"

    file_id="$(extract_json_field_with_python "$upload_json" "id")"
    [ -n "$file_id" ] || die "Impossibile estrarre file_id per chunk: $chunk_file"

    log "Invio prompt a ChatGPT per chunk: $chunk_basename"
    chatgpt \
      --no-resume \
      --save-raw "$raw_json_path" \
      -o "$corrected_file" \
      --file-id "$file_id" \
      -m "$MODEL" \
      -t "$TEMPERATURE" \
      -k "$MAX_OUTPUT_TOKENS" \
      --verbosity "$VERBOSITY" \
      "$PROMPT_TEXT" \
      2>>"$CHATGPT_RUN_STDERR"

    require_file "$corrected_file"
    log "Chunk corretto salvato: $corrected_file"
  done

  log "Avvio import_corrected_for_pdf_docx.py..."
  "$VENV_PYTHON" "${SCRIPT_DIR}/${IMPORT_SCRIPT}" \
    --input-dir "$LLM_CORRECTED_DIR" \
    --base-name "$VIDEO_BASENAME" \
    --output-dir "$LLM_MERGED_DIR" \
    --expected-slides "$expected_slides" \
    >"$IMPORT_STDOUT" 2>"$IMPORT_STDERR"

  require_file "$MERGED_SLIDE_TEXTS_JSON"
  log "JSON finale slide texts pronto: $MERGED_SLIDE_TEXTS_JSON"
}

# ============================================================
# ESECUZIONE SEQUENZIALE
# ============================================================
if [ "$screenshots_needed" -eq 1 ]; then
  log "Eseguo Screenshot_grabber..."
  task_screenshots
else
  log "Screenshot_grabber non necessario"
fi

require_dir "$SLIDES_DIR"
require_file "${SLIDES_DIR}/slides.csv"

if [ "$llm_pipeline_needed" -eq 1 ]; then
  log "Eseguo pipeline LLM..."
  run_llm_pipeline
else
  log "Pipeline LLM non necessaria"
fi

# ============================================================
# CHECK POST-TASK
# ============================================================
require_dir "$SLIDES_DIR"
require_file "${SLIDES_DIR}/slides.csv"
require_file "$MERGED_SLIDE_TEXTS_JSON"

# ============================================================
# STEP FINALE: PDF / DOCX
# ============================================================
log "Avvio slides_and_texts_to_pdf.py..."

"$VENV_PYTHON" "${SCRIPT_DIR}/${PDF_SCRIPT}" \
  --input-dir "$SLIDES_DIR" \
  --csv "slides.csv" \
  --slide-texts "$MERGED_SLIDE_TEXTS_JSON" \
  --output-base "$VIDEO_BASENAME" \
  >"$PDF_STDOUT" 2>"$PDF_STDERR"

move_final_outputs_to_workdir
cleanup_intermediate_srts

log "Pipeline completata."

echo
echo "Output principali:"
echo " - Video: $VIDEO_PATH"
echo " - SRT originale: $ORIGINAL_SRT"
echo " - Cartella slide: $SLIDES_DIR"
echo " - Chunk LLM: $LLM_CHUNKS_DIR"
echo " - Chunk corretti: $LLM_CORRECTED_DIR"
echo " - JSON finale slide texts: $MERGED_SLIDE_TEXTS_JSON"
echo " - PDF: $FINAL_PDF"
echo " - DOCX: $FINAL_DOCX"
echo " - Log: $LOG_DIR"