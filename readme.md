**Prima di lanciare SlideScribe crea la venv copiando nella root dello script i file create_venv.sh e requirements.txt nella cartella "tools".**

**Se vuoi usare SlideScribe.app un alias di SlideScribe deve essere posto in ~/.local/bin.**

**~/.zshrc deve quindi contenere la stringa export PATH="$PATH:$HOME/.local/bin"**

1. # Slidescribe

   Pipeline Bash per trasformare un video YouTube di una lezione in un output finale composto da slide, testo corretto tramite LLM e documenti finali consultabili.

   ## Cosa produce

   A partire da un video YouTube, lo script genera:

   - video locale in `.mkv`
   - sottotitoli automatici in `.srt`
   - cartella con le slide estratte dal video
   - chunk testuali preparati per l’LLM
   - chunk corretti dall’LLM
   - JSON finale con il testo associato a ciascuna slide
   - PDF finale
   - DOCX finale

   In pratica la pipeline è:

   **YouTube → video/subtitles → estrazione slide → export per LLM → correzione chunk → merge finale → PDF/DOCX**

   ------

   ## Scopo

   L’obiettivo dello script è automatizzare la conversione di una lezione video in un formato molto più leggibile e riutilizzabile.

   Lo script:

   1. scarica video e sottotitoli
   2. rileva i cambi slide e salva le immagini
   3. allinea la trascrizione alle slide
   4. divide il testo in chunk gestibili dall’LLM
   5. corregge i chunk via ChatGPT
   6. ricompone il materiale in un JSON finale
   7. genera PDF e DOCX con slide + testo

   L’orchestratore è scritto in Bash e coordina moduli Python e tool esterni.

   ------

   ## Architettura generale

   Lo script principale fa da regia. In sintesi:

   - risolve configurazione e input
   - controlla dipendenze e prerequisiti
   - costruisce i path di lavoro
   - decide quali step eseguire o saltare
   - lancia i moduli Python e i tool esterni
   - salva log separati per ogni fase
   - sposta gli output finali nelle posizioni corrette

   La logica è divisa così:

   - **Bash**: orchestrazione, validazioni, skip, checkpoint, logging
   - **Python modules**: lavorazioni specializzate
   - **yt-dlp**: download video e sottotitoli
   - **chatgpt wrapper**: upload file e correzione LLM

   ------

   ## Flusso della pipeline

   ### 1. Download video

   Se lo step è attivo, lo script scarica il video da YouTube tramite `yt-dlp` e lo salva nella `WORKDIR` in formato `.mkv`.

   Output principale:

   - `WORKDIR/VIDEO_BASENAME.mkv`

   ### 2. Download sottotitoli

   Lo script scarica i sottotitoli automatici, li converte in `.srt` e li normalizza in un file standard usato poi nel resto della pipeline.

   Output principale:

   - `WORKDIR/VIDEO_BASENAME.original.srt`

   ### 3. Estrazione slide

   Il modulo `Screenshot_grabber.py` analizza il video e genera:

   - la cartella delle slide
   - le immagini delle slide
   - `slides.csv`

   Questo step è il punto di aggancio fra timeline video e contenuti testuali.

   Output principali:

   - `WORKDIR/VIDEO_BASENAME slides/`
   - `WORKDIR/VIDEO_BASENAME slides/slides.csv`

   ### 4. Export per LLM

   Il modulo `export_for_llm.py` prende il file `.srt` e `slides.csv`, associa il testo alle slide e crea file chunkati per l’elaborazione LLM.

   Output principali:

   - `WORKDIR/llm_chunks/VIDEO_BASENAME.chunk_XXXX.txt`

   ### 5. Correzione LLM

   Ogni chunk viene caricato tramite il wrapper `chatgpt`, corretto dal modello e salvato in una directory dedicata.

   Output principali:

   - `WORKDIR/llm_corrected/VIDEO_BASENAME.chunk_XXXX.corrected.txt`
   - eventuali file raw JSON di debug

   ### 6. Merge finale

   Il modulo `import_corrected_for_pdf_docx.py` legge tutti i chunk corretti e costruisce il JSON finale con il testo delle slide.

   Output principale:

   - `WORKDIR/llm_merged/VIDEO_BASENAME.slide_texts.json`

   ### 7. Generazione PDF e DOCX

   Il modulo `slides_and_texts_to_pdf.py` usa cartella slide, `slides.csv` e JSON finale per creare i documenti finali.

   Output finali:

   - `WORKDIR/VIDEO_BASENAME.pdf`
   - `WORKDIR/VIDEO_BASENAME.docx`

   ------

   ## Logica di configurazione

   Lo script segue questa precedenza:

   **CLI > file config > default interni > prompt interattivo**

   Quindi:

   1. i default sono definiti nello script
   2. il file config sovrascrive i default
   3. le flag CLI sovrascrivono il config
   4. i valori mancanti possono essere chiesti a terminale, se non è attiva la modalità non interattiva

   Questo è il punto chiave da ricordare quando un valore finale non è quello atteso.

   ------

   ## Config file

   Lo script può lavorare con:

   - un file config di default nella cartella `config/`
   - un file config esplicito passato manualmente

   Il file viene caricato come shell config, quindi deve contenere assegnazioni compatibili con Bash.

   Esempio:

   ```bash
   WORKDIR="/Users/corax/Desktop/lezione1"
   YOUTUBE_URL="https://www.youtube.com/watch?v=..."
   VIDEO_BASENAME="lezione1"
   MODEL="gpt-5.4"
   TEMPERATURE="0.3"
   ROI_MODE="shared"
   ```

   Se il path del config non è assoluto, viene interpretato come relativo alla directory dello script.

   ------

   ## Ripartenza della pipeline

   Lo script è pensato per poter essere rilanciato senza rifare sempre tutto.

   ### Checkpoint

   Vengono riusati gli output già esistenti quando possibile. In particolare:

   - la presenza di `slides.csv` e cartella slide vale come checkpoint per la fase screenshot
   - la presenza del JSON merged vale come checkpoint per la pipeline LLM
   - la presenza di PDF e DOCX finali vale come checkpoint per il rendering finale

   ### Skip e restart

   La pipeline può essere fatta ripartire da step intermedi o con step selettivamente esclusi.

   In pratica puoi:

   - rifare solo la parte screenshot in poi
   - rifare solo la parte LLM
   - rigenerare solo PDF/DOCX

   ### Force

   Quando vuoi ignorare i checkpoint e rieseguire davvero gli step attivi, serve la modalità force.

   Caso tipico:

   - hai cambiato prompt, modulo Python o renderer finale e vuoi rigenerare lo step anche se esiste già output valido

   ------

   ## Validazioni e prerequisiti

   Prima dell’esecuzione vera, lo script controlla:

   - validità dei valori principali
   - disponibilità dei comandi esterni
   - esistenza della `.venv` e dei moduli Python
   - raggiungibilità e validità dell’URL YouTube, se serve il download
   - presenza degli artefatti necessari quando si saltano step manualmente

   Questo riduce gli errori “a metà pipeline” e rende più chiaro dove il flusso si rompe.

   ------

   ## Prompt LLM

   Se non viene passato un prompt esterno, lo script costruisce un prompt interno in italiano pensato per ripulire una trascrizione automatica.

   L’idea è questa:

   - correggere errori di trascrizione
   - sistemare termini tecnici
   - migliorare leggibilità e punteggiatura
   - non inventare contenuti
   - non alterare struttura e delimitatori del file

   Questa rigidità è essenziale, perché i moduli successivi si aspettano un formato molto preciso.

   Se viene fornito contesto terminologico, questo viene aggiunto al prompt.

   ------

   ## Directory e output

   Con una workdir tipo:

   - `WORKDIR=/path/run1`
   - `VIDEO_BASENAME=lezione1`

   la struttura logica è questa:

   ```text
   run1/
   ├── lezione1.mkv
   ├── lezione1.original.srt
   ├── lezione1.pdf
   ├── lezione1.docx
   ├── lezione1 slides/
   │   ├── slides.csv
   │   ├── [immagini slide]
   │   ├── lezione1.pdf
   │   └── lezione1.docx
   ├── llm_chunks/
   │   └── lezione1.chunk_*.txt
   ├── llm_corrected/
   │   ├── lezione1.chunk_*.corrected.txt
   │   └── [eventuali raw json]
   ├── llm_merged/
   │   └── lezione1.slide_texts.json
   └── logs/
       └── [log separati per fase]
   ```

   Nota pratica:

   - PDF e DOCX vengono creati prima nella cartella slide
   - poi vengono spostati nella `WORKDIR`

   ------

   ## Logging

   Ogni fase importante produce log separati.

   Tipicamente ci sono log per:

   - download video
   - download sottotitoli
   - screenshot
   - export LLM
   - upload ChatGPT
   - run ChatGPT
   - merge finale
   - generazione PDF/DOCX

   Questo rende molto più facile capire dove la pipeline ha fallito.

   ------

   ## Ordine reale di esecuzione

   A grandi linee il `main` segue questo schema:

   1. definizione dei default
   2. parsing argomenti
   3. eventuale caricamento config
   4. riapplicazione override CLI
   5. validazione e normalizzazione
   6. controllo dipendenze
   7. risoluzione input e path
   8. calcolo checkpoint
   9. verifica prerequisiti per gli skip
   10. esecuzione pipeline attiva
   11. cleanup finale
   12. riepilogo output

   Questo è utile da ricordare quando devi capire in che fase viene deciso davvero un certo comportamento.

   ------

   ## Punti pratici da ricordare

   - `WORKDIR` e `VIDEO_BASENAME` determinano quasi tutta la struttura dei file
   - `slides.csv` è il perno della parte slide
   - `VIDEO_BASENAME.original.srt` è il perno della parte testo sorgente
   - `llm_chunks` contiene gli input per l’LLM
   - `llm_corrected` contiene gli output corretti
   - `llm_merged/*.slide_texts.json` è il file finale usato dal renderer
   - PDF e DOCX finali dipendono da slide + `slides.csv` + JSON merged
   - quasi ogni problema è rintracciabile dai log

   ------

   # Moduli

   ## Screenshot_grabber.py

   Modulo responsabile dell’estrazione delle slide dal video. Il suo compito non è fare OCR o interpretare il contenuto, ma individuare i cambi slide, salvare le immagini corrette e produrre il file `slides.csv` che verrà poi usato dal resto della pipeline. Il modulo legge un video locale, permette una selezione iniziale interattiva delle aree rilevanti, analizza il video a intervalli regolari e salva una sequenza pulita di slide rettificate.

   ### Funzione del modulo

   `Screenshot_grabber.py` fa tre cose principali:

   1. definisce **che cosa salvare**: la slide vera e propria
   2. definisce **dove rilevare il cambio**: area trigger
   3. produce **gli artefatti base** per il resto della pipeline:
      - immagini delle slide
      - `slides.csv`

   È quindi il modulo che trasforma un video continuo in una sequenza discreta di slide indicizzate nel tempo.

   ### Input

   Input principale:

   - file video locale

   Input interattivi:

   - quadrilatero della slide da catturare
   - opzionale ROI trigger separata

   Parametri di controllo:

   - frequenza di campionamento del video
   - soglie per rilevare il cambio slide
   - tempo minimo tra due slide
   - parametri di stabilizzazione
   - parametri di deduplicazione
   - modalità di salvataggio (`crop` o `full`)

   ### Output

   Il modulo genera:

   - immagini `slide_XXX_timestamp.png`
   - file `slides.csv`

   Il CSV contiene per ogni slide:

   - indice slide
   - timestamp in secondi
   - timestamp formattato
   - nome file immagine

   Questo file è il riferimento usato poi per allineare il testo della trascrizione alle slide.

   ### Logica operativa

   Il modulo lavora così:

   1. apre il video con OpenCV
   2. legge FPS, numero frame e durata stimata
   3. chiede all’utente di selezionare i 4 vertici della slide
   4. ordina automaticamente i 4 punti
   5. calcola una trasformazione prospettica per rettificare la slide
   6. definisce la ROI trigger:
      - separata, se richiesta
      - altrimenti bounding box rettangolare della quadrilatera slide
   7. campiona il video a intervalli regolari
   8. confronta la ROI trigger corrente con quella dell’ultima slide salvata
   9. quando rileva un cambio plausibile, prova a verificarne la stabilità su alcuni campioni successivi
   10. salva la nuova slide
   11. a fine scansione, deduplica eventuali quasi-duplicati
   12. rinumera i file finali e scrive `slides.csv`

   ### Selezione della slide

   La slide non viene selezionata come rettangolo, ma come **quadrilatero a 4 punti**. Questo permette di lavorare anche quando la slide nel video è prospetticamente deformata.

   Dopo il quarto click il modulo:

   - riordina i punti automaticamente in ordine `TL, TR, BR, BL`
   - mostra una preview rettificata
   - aspetta conferma dell’utente

   Questo è uno dei miglioramenti chiave del modulo, perché evita di salvare screenshot storti o con proporzioni sbagliate.

   ### Ordinamento automatico dei punti

   Se l’utente clicca i vertici in ordine casuale, il modulo li riordina automaticamente.

   La logica è:

   - calcolo del centro dei 4 punti
   - ordinamento per angolo rispetto al centro
   - scelta del punto iniziale più plausibile
   - controllo del verso del poligono
   - normalizzazione finale nell’ordine:
     - top-left
     - top-right
     - bottom-right
     - bottom-left

   Questo evita dipendenze dall’ordine di click dell’utente.

   ### Rettifica prospettica

   Una volta definita la quadrilatera slide, il modulo calcola una `perspective transform` con OpenCV e la converte in un rettangolo frontale.

   La dimensione dell’immagine di output non è fissa: viene stimata dinamicamente sulla base della geometria reale del quadrilatero, usando la media delle lunghezze dei lati opposti.

   Questo rende il salvataggio più fedele alla slide reale e meno dipendente da hardcode arbitrari.

   ### ROI trigger

   Il rilevamento del cambio slide non avviene necessariamente sulla stessa area che poi viene salvata.

   Ci sono due modalità:

   - **trigger coincidente**: viene usato il bounding box rettangolare della quadrilatera slide
   - **trigger separato**: l’utente seleziona un’altra ROI rettangolare dedicata al rilevamento del cambio

   La modalità trigger separato è utile quando:

   - la slide è prospetticamente complessa ma c’è una zona rettangolare pulita più stabile per il rilevamento
   - vuoi evitare falsi positivi dovuti a elementi periferici
   - vuoi usare una zona più sensibile al cambio contenuto

   ### Rilevamento del cambio slide

   Per capire se una slide è cambiata, il modulo confronta due immagini della ROI trigger:

   - ultima ROI salvata
   - ROI corrente campionata

   Il confronto usa tre metriche:

   - **SSIM**
   - **mean diff**
   - **changed ratio**

   La slide viene considerata cambiata quando:

   - l’SSIM scende sotto soglia
   - e contemporaneamente almeno una tra `mean diff` o `changed ratio` supera la propria soglia

   Questa logica è volutamente più robusta di un semplice confronto pixel a pixel.

   ### Stabilizzazione

   Quando viene rilevato un cambio, il modulo non salva subito il frame corrente in modo cieco. Prima controlla alcuni campioni successivi per verificare che la nuova slide sia davvero stabile.

   La funzione `maybe_extract_candidate()` cerca nei frame successivi una versione della slide che abbia una similarità abbastanza alta rispetto al nuovo stato rilevato.

   In pratica serve a evitare di salvare:

   - transizioni animate
   - frame intermedi
   - cambi non ancora assestati

   ### Frequenza di campionamento

   Il video non viene analizzato frame per frame, ma a intervalli regolari definiti da `sample_every_sec`.

   Questo riduce il costo computazionale e, per slide didattiche, è normalmente sufficiente.

   Dal sampling derivano due concetti importanti:

   - `step_frames`: quanti frame saltare tra un controllo e l’altro
   - `min_gap_samples`: quanti campioni devono passare prima di poter accettare una nuova slide

   ### Tempo minimo tra slide

   Il parametro `min_slide_duration_sec` impone una distanza minima tra due slide salvate.

   Serve per limitare:

   - duplicati ravvicinati
   - falsi cambi dovuti a animazioni brevi
   - rumore video

   ### Modalità di salvataggio

   Il modulo supporta due modalità:

   - `crop`: salva la slide rettificata
   - `full`: salva l’intero frame originale

   Nella pipeline Slidescribe il comportamento sensato è in genere `crop`, perché il resto del flusso si aspetta una cartella di slide pulite.

   ### Prima slide

   Per default la prima slide viene salvata subito appena inizia l’analisi.

   Questo comportamento può essere disattivato con l’opzione che impedisce il salvataggio immediato della prima slide.

   ### Deduplicazione finale

   Dopo la scansione del video, il modulo esegue una deduplicazione ex post tra slide consecutive già salvate.

   Anche qui usa confronto immagini con due criteri:

   - SSIM molto alto
   - oppure differenza media molto bassa

   Se una slide è considerata duplicata:

   - il file immagine viene rimosso
   - il record non entra nella sequenza finale

   Dopo la deduplicazione, i file vengono rinumerati in ordine progressivo coerente.

   ### Naming dei file

   Le immagini vengono salvate con questo schema:

   ```text
   slide_001_00-00-12_345.png
   ```

   Quindi il nome contiene:

   - indice progressivo
   - timestamp della slide

   Questo è utile sia per leggibilità manuale sia per debugging.

   ### Funzioni principali

   Le funzioni chiave del modulo sono:

   - `interactive_select_quad()`
     Selezione manuale dei 4 vertici della slide.
   - `order_quad_points()`
     Riordina automaticamente i vertici del quadrilatero.
   - `warp_quad_to_rect()`
     Rettifica la slide tramite trasformazione prospettica.
   - `compare_images()`
     Calcola le metriche di similarità fra due immagini.
   - `is_slide_change()`
     Decide se il cambio è sufficiente per considerare una nuova slide.
   - `maybe_extract_candidate()`
     Cerca una versione stabile della nuova slide nei campioni successivi.
   - `deduplicate_records()`
     Elimina quasi-duplicati a fine elaborazione.
   - `write_csv()`
     Scrive `slides.csv`.
   - `extract_slides()`
     Funzione principale del modulo: coordina l’intero processo.

   ### Dipendenze principali

   Il modulo usa:

   - `opencv-python` (`cv2`) per video, UI, crop e trasformazione prospettica
   - `numpy` per geometria e manipolazione punti
   - `scikit-image` per SSIM
   - librerie standard Python per path, CSV, parsing argomenti e dataclass

   ### Ruolo nella pipeline complessiva

   Questo modulo è uno snodo critico perché a valle tutto dipende dalla qualità delle slide prodotte.

   Se qui sbagli:

   - il numero di slide
   - il timestamp di cambio
   - la pulizia delle immagini
   - la deduplicazione

   si propagano errori anche in:

   - segmentazione dei chunk
   - allineamento testo/slide
   - output finale PDF/DOCX

   ### Limiti pratici

   Il modulo non fa magia. Funziona bene se:

   - la zona slide è abbastanza stabile nel video
   - i cambi slide sono visibili nella ROI trigger
   - non ci sono troppe animazioni, dissolvenze o overlay persistenti

   Può degradare quando:

   - il relatore copre spesso la slide
   - ci sono transizioni lunghe
   - il player/video introduce elementi mobili nell’area trigger
   - la ROI scelta è troppo rumorosa o troppo piccola

   ### Riassunto rapido

   In una frase: `Screenshot_grabber.py` prende un video, fa scegliere all’utente la slide da catturare, rileva automaticamente i cambi slide nel tempo, salva immagini rettificate e produce `slides.csv`, cioè la base temporale e visiva su cui si appoggia tutto il resto della pipeline.

   

   ## export_for_llm.py

   Modulo incaricato di prendere la trascrizione `.srt` e la timeline delle slide (`slides.csv`), associare il testo a ciascuna slide in base ai timestamp e generare file chunkati pronti per essere inviati all’LLM. Non corregge il testo e non produce output finali: il suo compito è creare un ponte ordinato tra trascrizione grezza e fase di correzione LLM.

   ### Funzione del modulo

   `export_for_llm.py` serve a trasformare due sorgenti eterogenee:

   - una trascrizione temporale in formato SRT
   - una sequenza di slide con timestamp

   in una struttura testuale molto più rigida e controllata, fatta di chunk con blocchi slide espliciti.

   In pratica il modulo:

   1. legge e pulisce l’SRT
   2. legge `slides.csv`
   3. assegna ogni blocco SRT alla slide corretta
   4. unisce le righe per slide
   5. rimuove duplicazioni locali e overlap tra slide consecutive
   6. suddivide il materiale in chunk
   7. scrive file `.txt` strutturati per l’LLM

   ### Input

   Input richiesti:

   - file `.srt` sorgente
   - file `slides.csv`
   - directory output per i chunk
   - `base_name` del progetto

   Parametri di controllo:

   - `chunk_size`: numero di slide per chunk
   - `empty_placeholder`: testo da usare per slide senza contenuto

   ### Output

   Il modulo genera una serie di file `.txt` in output, con naming del tipo:

   ```text
   BASE.chunk_001_slides_0001_0020.txt
   ```

   Ogni file contiene:

   - intestazione chunk
   - una sequenza ordinata di blocchi slide
   - delimitatori espliciti `BEGIN/END`
   - una sezione `TEXT:` per ogni slide

   La struttura è pensata per essere il più possibile stabile e machine-friendly, così che l’LLM possa correggere il contenuto senza rompere il formato.

   ### Ruolo nella pipeline

   Questo modulo si colloca tra:

   - `Screenshot_grabber.py`, che produce `slides.csv`
   - la fase LLM, che corregge i chunk

   È quindi il modulo che converte la timeline visiva delle slide e la timeline testuale della trascrizione in un dataset intermedio coerente per la correzione automatica.

   ### Logica operativa generale

   Il flusso del modulo è questo:

   1. legge `slides.csv`
   2. legge e parse il file SRT
   3. crea una mappa `slide -> blocchi testuali`
   4. comprime e ripulisce il testo di ogni slide
   5. rimuove ripetizioni fra slide adiacenti
   6. divide le slide in gruppi di dimensione fissa
   7. scrive i file chunk finali

   ### Parsing dell’SRT

   Il parser SRT è volutamente tollerante. Non assume un file perfetto e gestisce diversi casi realistici.

   Accetta:

   - blocchi SRT standard con indice numerico
   - blocchi senza indice ma con riga timestamp valida
   - righe timestamp con eventuali attributi extra in coda

   Scarta invece:

   - blocchi troppo corti
   - blocchi senza testo
   - blocchi malformati
   - blocchi con durata inferiore a `MIN_BLOCK_DURATION_SEC`

   Questo è importante perché molte sbobinature automatiche contengono rumore, blocchi vuoti o segmenti inutili.

   ### Normalizzazione del testo

   Il testo dei blocchi viene ripulito subito con `normalize_whitespace()`.

   Questa funzione:

   - rimuove BOM e caratteri anomali comuni
   - uniforma i ritorni a capo
   - elimina righe vuote
   - comprime gli spazi multipli
   - restituisce una stringa singola pulita

   Il risultato non è ancora “corretto” linguisticamente: è solo reso coerente e più trattabile.

   ### Parsing di `slides.csv`

   Il modulo legge il CSV prodotto da `Screenshot_grabber.py` e costruisce una lista ordinata di oggetti `Slide`.

   Per robustezza, non si affida a un solo nome di colonna: accetta più alias possibili per campi come:

   - indice slide
   - timestamp in secondi
   - timestamp formattato
   - filename immagine

   Questo rende il modulo più tollerante a leggere variazioni del CSV, purché la semantica delle colonne resti coerente.

   ### Assegnazione blocchi SRT alle slide

   L’associazione avviene usando l’orario di inizio del blocco SRT.

   Per ogni blocco:

   - si prende `start_sec`
   - si cerca la slide la cui finestra temporale contiene quel timestamp
   - il testo del blocco viene assegnato a quella slide

   La logica è implementata da `find_slide_for_time()`:

   - tra una slide e la successiva, il testo appartiene alla slide corrente
   - oltre l’ultima soglia, tutto finisce sull’ultima slide

   Questa scelta è semplice ma sensata, perché una slide rappresenta implicitamente l’intervallo che va dal suo timestamp fino al timestamp della slide successiva.

   ### Aggregazione per slide

   Una volta assegnati i blocchi, il modulo costruisce una struttura:

   ```text
   slide_index -> [lista di frammenti testuali]
   ```

   A quel punto ogni slide può avere:

   - nessun testo
   - un solo blocco
   - più blocchi consecutivi

   Il problema successivo diventa quindi pulire e fondere questi frammenti in un unico testo coerente per slide.

   ### Rimozione overlap all’interno della slide

   Le trascrizioni automatiche contengono spesso sovrapposizioni tra un blocco e il successivo. Per esempio una frase può finire in un blocco e ricominciare quasi uguale nel blocco dopo.

   Per questo il modulo applica `clean_slide_lines()`.

   La logica è:

   - confronta ogni riga con la precedente
   - cerca un overlap fra coda della precedente e inizio della successiva
   - rimuove la parte duplicata
   - conserva solo il contenuto realmente nuovo

   Questo riduce le ripetizioni tipiche da caption automatiche.

   ### Algoritmo di overlap

   La rimozione overlap è implementata in due stadi:

   1. confronto a livello parole, cercando un match esatto fra suffisso della riga precedente e prefisso della riga corrente
   2. fallback su token normalizzati, più tollerante verso punteggiatura e piccoli slittamenti

   Funzioni coinvolte:

   - `_tokenize_for_overlap()`
   - `_find_overlap_token_count()`
   - `strip_overlap()`

   Questo approccio è più robusto di una semplice rimozione stringa-grezza, perché regge meglio casi di sottotitoli quasi uguali ma non identici carattere per carattere.

   ### Composizione del testo finale di una slide

   Dopo la pulizia dei frammenti, `join_slide_text()`:

   - normalizza di nuovo le righe
   - elimina vuoti residui
   - concatena tutto in una singola stringa
   - usa `empty_placeholder` se la slide non ha contenuto

   Quindi ogni slide esce da questo passaggio con un solo testo compatto.

   ### Deduplicazione fra slide consecutive

   Il modulo non si ferma alla pulizia interna della singola slide. Fa anche una seconda passata fra slide consecutive.

   Motivo: a volte lo stesso testo deborda dalla slide precedente a quella successiva, specialmente se il cambio slide avviene vicino ai bordi temporali di alcuni blocchi SRT.

   La funzione `dedupe_across_slides()` fa questo:

   - prende il testo della slide precedente
   - confronta il testo della slide corrente
   - rimuove l’eventuale prefisso già presente in coda alla slide precedente
   - se il testo corrente è sostanzialmente tutto contenuto nella coda della precedente, lo svuota del tutto

   Questo passaggio è cruciale per evitare output finali dove due slide adiacenti ripetono quasi la stessa frase.

   ### Chunking

   Una volta costruito il testo finale per ogni slide, il modulo divide la lista delle slide in gruppi di dimensione fissa tramite `chunked()`.

   Il chunking non è semantico: è puramente strutturale. Serve a:

   - limitare la quantità di testo per richiesta LLM
   - rendere più semplice retry e ripartenza
   - mantenere una corrispondenza chiara fra gruppi di slide e file processati

   Ogni chunk contiene un intervallo continuo di slide.

   ### Formato dei chunk

   I file chunk sono scritti con una struttura rigida come questa:

   ```text
   ===== BEGIN CHUNK 001 =====
   
   ----- BEGIN SLIDE 0001 -----
   TEXT:
   [testo]
   
   ----- END SLIDE 0001 -----
   ```

   Questa struttura è deliberata.

   Serve a dare all’LLM un input con:

   - delimitatori forti
   - segmentazione esplicita per slide
   - formato facilmente verificabile in import

   In altre parole, il file non è pensato per la lettura umana elegante, ma per massimizzare la robustezza della pipeline.

   ### Slide vuote

   Se una slide non riceve testo oppure il testo viene completamente eliminato in fase di deduplica, il modulo usa `empty_placeholder`.

   Di default è stringa vuota. Questo vuol dire che una slide può comparire nel chunk anche senza contenuto, ma resta comunque presente come entità strutturale.

   È una scelta corretta: il numero e l’ordine delle slide vanno preservati anche quando non c’è testo utile.

   ### Logging e diagnostica

   Il modulo scrive i messaggi diagnostici su `stderr` tramite `eprint()`.

   Tipicamente logga:

   - numero di slide trovate
   - numero di blocchi SRT validi
   - numero di blocchi scartati
   - numero di slide con testo non vuoto
   - numero di chunk scritti
   - path di ogni file generato

   Questo lo rende molto facile da integrare in una pipeline orchestrata da Bash con log distinti.

   ### Strutture dati principali

   Le due dataclass centrali sono:

   - `Slide`
   - `SRTBlock`

   `Slide` rappresenta una slide con:

   - indice
   - timestamp in secondi
   - timestamp formattato
   - filename

   `SRTBlock` rappresenta un blocco della trascrizione con:

   - indice
   - start time
   - end time
   - testo

   Sono strutture minimali ma sufficienti per mantenere leggibilità interna nel codice.

   ### Funzioni principali

   Le funzioni più importanti del modulo sono:

   - `parse_srt()`
     Legge e valida il file SRT in modo tollerante.
   - `parse_slides_csv()`
     Legge il CSV delle slide e costruisce la sequenza ordinata.
   - `find_slide_for_time()`
     Decide a quale slide appartiene un blocco temporale.
   - `aggregate_text_by_slide()`
     Costruisce la mappa `slide -> frammenti testuali`.
   - `clean_slide_lines()`
     Pulisce overlap e ripetizioni dentro la stessa slide.
   - `join_slide_text()`
     Produce il testo finale di una slide.
   - `dedupe_across_slides()`
     Elimina il testo ridondante fra slide consecutive.
   - `chunked()`
     Divide la sequenza di slide in blocchi di dimensione fissa.
   - `write_chunk_file()`
     Scrive il file `.txt` finale nel formato previsto.
   - `main()`
     Coordina l’intero flusso.

   ### Dipendenze principali

   Il modulo usa solo librerie standard Python:

   - `argparse`
   - `csv`
   - `re`
   - `sys`
   - `dataclasses`
   - `pathlib`
   - `typing`

   Questo è un vantaggio pratico: è leggero, portabile e non richiede dipendenze esterne pesanti.

   ### Punti forti del modulo

   I punti migliori della logica sono questi:

   - parser SRT abbastanza tollerante da reggere input sporchi
   - deduplica sia intra-slide sia inter-slide
   - formato chunk rigido e prevedibile
   - dipendenze minime
   - output molto adatto a una pipeline LLM controllata

   ### Limiti pratici

   Il modulo resta comunque euristico. Può degradare in questi casi:

   - sottotitoli fortemente rumorosi o temporizzati male
   - timestamp slide inaccurati in `slides.csv`
   - casi in cui il parlato anticipa o ritarda molto rispetto al cambio slide
   - overlap più complessi di quelli eliminabili con suffisso/prefisso lineare

   In particolare, l’assegnazione usa `start_sec` del blocco SRT e non il midpoint o una distribuzione più sofisticata del testo sull’intervallo. È una scelta semplice e robusta, ma non perfetta in tutti i casi.

   ### Riassunto rapido

   In una frase: `export_for_llm.py` prende la trascrizione grezza e la timeline delle slide, le allinea temporalmente, pulisce sovrapposizioni e ridondanze, poi esporta file chunkati in un formato rigido pronto per la correzione LLM.

   

   ## import_corrected_for_pdf_docx.py

   Modulo incaricato di prendere i file `.corrected.txt` restituiti dalla fase LLM, verificarne rigorosamente la struttura e ricomporli in un unico output finale per slide. Non fa correzioni linguistiche, non riassegna testo alle slide e non genera direttamente PDF o DOCX: il suo compito è chiudere la fase LLM e trasformare i chunk corretti in un artefatto unico, coerente e pronto per il renderer finale.

   ### Funzione del modulo

   `import_corrected_for_pdf_docx.py` serve a fare da **barriera di validazione** tra:

   - i chunk corretti dall’LLM
   - il renderer finale che costruirà PDF e DOCX

   In pratica il modulo:

   1. legge tutti i file `.corrected.txt`
   2. controlla che ogni file rispetti il formato atteso
   3. estrae i blocchi slide dal testo
   4. ricompone una mappa unica `slide_index -> text`
   5. verifica che non manchi nessuna slide e che non ce ne siano di duplicate o fuori range
   6. scrive un JSON finale machine-friendly
   7. scrive anche un TXT finale utile per debug umano

   È quindi un modulo piccolo ma critico: impedisce che output LLM formalmente rotti passino silenziosamente a valle. fileciteturn3file0

   ### Input

   Input richiesti:

   - `input-dir`: cartella contenente i file `.corrected.txt`
   - `base-name`: nome base del progetto/output
   - `output-dir`: cartella dove scrivere gli artefatti finali ricomposti
   - `expected-slides`: numero totale atteso di slide

   Input opzionale:

   - `glob`: pattern con cui cercare i file corretti, default `*.corrected.txt`

   Dal punto di vista logico, il modulo assume che i chunk provengano dalla fase precedente e abbiano una struttura molto precisa con delimitatori di chunk e slide.

   ### Output

   Il modulo produce due file finali:

   - `BASE.slide_texts.json`
   - `BASE.slide_texts.txt`

   Il JSON è l’output principale per il resto della pipeline. Contiene:

   - `base_name`
   - `total_slides`
   - array `slides`
   - per ogni slide: `slide_index` e `text`

   Il TXT invece è un dump leggibile da umano, con lo stesso schema di blocchi slide, utile per controlli rapidi e debugging.

   ### Ruolo nella pipeline

   Questo modulo si colloca **dopo** la correzione LLM e **prima** del renderer PDF/DOCX.

   La sequenza logica è:

   - `export_for_llm.py` crea chunk rigidamente formattati
   - l’LLM li corregge cercando di preservare la struttura
   - `import_corrected_for_pdf_docx.py` verifica che la struttura sia rimasta integra e ricompone tutto
   - il renderer finale usa il JSON per associare i testi alle immagini delle slide

   Quindi questo modulo non “migliora” il testo: controlla che il testo corretto sia ancora utilizzabile in modo affidabile.

   ### Logica operativa generale

   Il flusso del modulo è questo:

   1. parse argomenti CLI
   2. verifica esistenza di input e validità di `expected-slides`
   3. cerca i file corretti nella cartella input
   4. li ordina per numero chunk
   5. parse ogni file e ne estrae le slide
   6. costruisce una mappa globale delle slide
   7. controlla duplicati tra chunk
   8. controlla completezza rispetto al numero atteso di slide
   9. scrive JSON finale
   10. scrive TXT di debug

   È un flusso lineare e molto conservativo: appena trova incoerenze, fallisce.

   ### Ordinamento dei file chunk

   I file non vengono letti in ordine alfabetico puro, ma ordinati tramite `extract_chunk_number()`.

   La funzione cerca nel nome file il pattern:

   ```text
   chunk_001
   ```

   ed estrae il numero del chunk con regex.

   Questo è importante perché:

   - impedisce dipendenze da ordinamenti lessicografici ambigui
   - forza un ordinamento coerente con la sequenza della pipeline
   - fa fallire subito se un file ha naming incompatibile

   Se il nome non contiene un numero chunk valido, il modulo alza un errore invece di tentare interpretazioni creative.

   ### Parsing dei file corretti

   La funzione centrale è `parse_corrected_chunk()`.

   Per ogni file:

   1. legge il contenuto in UTF-8 con tolleranza BOM (`utf-8-sig`)
   2. normalizza i newline
   3. verifica che esistano i marker:
      - `===== BEGIN CHUNK NNN =====`
      - `===== END CHUNK NNN =====`
   4. cerca tutti i blocchi slide con regex strutturata
   5. estrae per ogni slide:
      - indice slide
      - testo associato
   6. pulisce il testo in modo molto conservativo

   La filosofia è chiara: il modulo si fida poco dell’output LLM e pretende una struttura quasi identica a quella prevista.

   ### Validazione dei marker chunk

   Prima ancora di cercare le slide, il modulo controlla che il file abbia:

   - un `BEGIN CHUNK`
   - un `END CHUNK`

   Se uno dei due manca, il file è considerato invalido e il parsing fallisce subito.

   Questa scelta è corretta perché un output LLM che perde i delimitatori di chunk è già strutturalmente sospetto.

   ### Regex dei blocchi slide

   Il parser slide usa una regex molto rigida. Il formato atteso è questo:

   ```text
   ----- BEGIN SLIDE 0001 -----
   TEXT:
   ...
   ----- END SLIDE 0001 -----
   ```

   Condizioni implicite importanti:

   - l’indice di apertura e chiusura deve coincidere
   - deve esserci la riga `TEXT:` esatta
   - la struttura dei delimitatori deve essere rispettata

   Il testo interno può essere multilinea, ma il guscio esterno deve rimanere invariato.

   Questo è il motivo per cui la fase LLM deve essere istruita a non alterare delimitatori e layout.

   ### Pulizia del testo estratto

   Dopo l’estrazione del blocco, il testo viene ripulito in modo volutamente minimo:

   - rimozione di newline spurii iniziali/finali
   - `strip()` finale degli spazi esterni
   - nessuna alterazione del contenuto interno

   Questa è una scelta intelligente. In questa fase non bisogna più “interpretare” o “migliorare” il testo: bisogna solo ricomporlo senza introdurre nuove trasformazioni.

   ### Validazione interna al singolo file

   Dopo il parsing di tutte le slide di un file, il modulo controlla due cose:

   1. che non ci siano slide duplicate nello stesso file
   2. che l’ordine degli indici sia strettamente crescente

   Se ad esempio trova:

   - due volte la slide 0012 nello stesso chunk
   - oppure 0012 seguita da 0011

   fallisce immediatamente.

   Questo serve a intercettare output LLM in cui il formato sembra quasi giusto ma in realtà la sequenza è stata corrotta.

   ### Costruzione della mappa globale

   Nel `main()` ogni chunk parsato contribuisce ad alimentare una struttura unica:

   ```text
   slide_map: Dict[int, str]
   ```

   cioè una mappa tra numero slide e testo finale.

   Quando una slide arriva da un file chunk, il modulo controlla che quell’indice non sia già presente nella mappa. Se lo è, significa che due file diversi stanno fornendo la stessa slide: errore immediato.

   Questa validazione intercetta:

   - overlap tra chunk esportati male
   - duplicazioni dovute a output LLM corrotti
   - errori di naming o ricombinazione a monte

   ### Validazione di completezza globale

   Una volta letti tutti i file, il modulo confronta:

   - insieme delle slide trovate
   - insieme delle slide attese, da `1` a `expected_slides`

   Calcola quindi:

   - `missing`: slide attese ma assenti
   - `extra`: slide presenti ma fuori range atteso

   Se una delle due liste non è vuota, il modulo fallisce.

   Questo passaggio è essenziale perché il renderer finale si aspetta una corrispondenza completa e ordinata tra immagini slide e testi.

   ### Gestione dei missing

   Se mancano slide, il modulo stampa un errore con i primi indici mancanti formattati a quattro cifre.

   Esempio logico:

   ```text
   [ERRORE] Mancano slide attese: 0007, 0008, 0041 ...
   ```

   Non tenta recovery automatici, non prova a riempire con stringhe vuote e non inventa nulla. Fallisce.

   Questa è la scelta giusta, perché la mancanza di una slide è un problema strutturale che va corretto a monte.

   ### Gestione degli extra

   Se trova slide fuori range, il modulo fa lo stesso: logga l’errore e fallisce.

   Questo protegge da casi in cui:

   - il numero atteso di slide è sbagliato
   - i chunk contengono indici corrotti
   - l’LLM ha accidentalmente duplicato o inventato una slide

   ### JSON finale

   Una volta superate tutte le validazioni, il modulo costruisce il payload JSON finale con questa forma logica:

   ```json
   {
     "base_name": "...",
     "total_slides": 150,
     "slides": [
       {"slide_index": 1, "text": "..."},
       {"slide_index": 2, "text": "..."}
     ]
   }
   ```

   Le slide vengono sempre scritte nell’ordine da `1` a `expected_slides`.

   Questo è importante: il JSON non dipende più dall’ordine dei file chunk letti, ma da una sequenza canonica esplicita.

   ### TXT di debug

   Oltre al JSON, il modulo produce anche un file di testo con la stessa articolazione in blocchi slide.

   La funzione `write_debug_txt()` scrive per ogni slide:

   - `BEGIN SLIDE`
   - `TEXT:`
   - contenuto
   - `END SLIDE`

   Questo file non è pensato per la macchina, ma per controlli manuali veloci.

   È molto utile quando vuoi:

   - leggere il testo finale senza aprire il JSON
   - confrontare rapidamente il risultato con i chunk originali
   - fare debugging di merge e validazione

   ### Logging e diagnostica

   Il modulo scrive i messaggi su `stderr` tramite `eprint()`.

   Tipicamente logga:

   - quanti file corretti sono stati trovati
   - quale file sta parsando
   - dove ha scritto JSON e TXT finali
   - eventuali errori strutturali
   - messaggio finale di completamento

   Questo lo rende facile da integrare in un orchestratore Bash con redirect separati di stdout/stderr.

   ### Strutture dati principali

   La dataclass centrale è:

   - `ParsedSlide`

   Contiene solo:

   - `slide_index`
   - `text`

   È volutamente minimale, perché in questa fase non servono più timestamp o metadati visivi. A questo punto conta solo la corrispondenza stabile tra indice slide e testo finale.

   ### Funzioni principali

   Le funzioni più importanti del modulo sono:

   - `parse_args()`
     Gestisce gli argomenti CLI.
   - `normalize_newlines()`
     Uniforma i ritorni a capo prima del parsing.
   - `extract_chunk_number()`
     Estrae il numero chunk dal nome file per ordinamento e validazione.
   - `parse_corrected_chunk()`
     Valida e parse un singolo file corretto.
   - `write_debug_txt()`
     Scrive il dump testuale finale per debug umano.
   - `main()`
     Coordina scansione file, merge globale, validazioni e scrittura output.

   ### Dipendenze principali

   Il modulo usa solo librerie standard Python:

   - `argparse`
   - `json`
   - `re`
   - `sys`
   - `dataclasses`
   - `pathlib`
   - `typing`

   Questo è coerente con il suo ruolo: è un validatore/ricompositore leggero, non ha bisogno di dipendenze esterne pesanti.

   ### Punti forti del modulo

   I punti migliori sono questi:

   - struttura semplice e leggibile
   - validazione severa dove serve davvero
   - nessuna trasformazione linguistica invasiva in questa fase
   - output doppio: JSON per macchina, TXT per umano
   - fallimento esplicito su errori strutturali

   In altre parole, è un buon modulo da “guardrail”: protegge il resto della pipeline da output LLM formalmente rotti.

   ### Limiti pratici

   I limiti sono soprattutto una conseguenza della scelta deliberata di essere rigido.

   Per esempio:

   - se l’LLM altera anche leggermente i delimitatori, il parsing può fallire
   - non c’è recovery euristico di strutture quasi corrette
   - non prova a sanare chunk mancanti o slide mancanti
   - dipende dal naming `chunk_XXX` nei file input

   Questo però non è davvero un difetto nel contesto di Slidescribe: è una scelta sensata, perché qui la priorità è affidabilità strutturale, non permissività.

   ### Riassunto rapido

   In una frase: `import_corrected_for_pdf_docx.py` prende i chunk corretti dall’LLM, ne verifica rigorosamente formato e completezza, li ricompone in una mappa unica slide→testo e produce il JSON finale che verrà usato per generare PDF e DOCX.

   

   ## slides_and_texts_to_pdf.py

   Modulo finale della pipeline, incaricato di prendere la timeline delle slide (`slides.csv`), il testo finale per slide (`slide_texts.json`) e le immagini delle slide, e trasformare tutto in due documenti consultabili: un PDF e un DOCX. Non fa più validazione strutturale della fase LLM e non riassegna il testo alle slide: in questa fase il materiale è considerato già consolidato e viene impaginato in un formato leggibile per uso umano.

   ### Funzione del modulo

   `slides_and_texts_to_pdf.py` è il renderer finale di Slidescribe.

   In pratica:

   1. legge il CSV delle slide
   2. legge il JSON finale con i testi
   3. unisce metadati slide e testo in una struttura unica
   4. genera un PDF orizzontale con immagine slide + testo
   5. genera un DOCX orizzontale con la stessa logica

   È quindi il punto in cui la pipeline smette di produrre artefatti tecnici intermedi e costruisce documenti finali realmente fruibili.

   ### Input

   Input richiesti:

   - cartella input contenente `slides.csv` e le immagini slide
   - file JSON con il testo finale per slide

   Parametri CLI principali:

   - `--input-dir`: cartella delle slide
   - `--csv`: nome del file CSV delle slide
   - `--slide-texts`: path del JSON finale
   - `--output-base`: nome base dei file output

   Dal punto di vista logico il modulo si aspetta tre sorgenti coerenti tra loro:

   - immagini slide
   - CSV con indice/timestamp/filename
   - JSON con mappa `slide_index -> text`

   Se una di queste tre componenti è incoerente o incompleta, la resa finale degrada o fallisce.

   ### Output

   Il modulo genera due file:

   - `OUTPUT_BASE.pdf`
   - `OUTPUT_BASE.docx`

   Entrambi vengono scritti nella cartella input delle slide.

   La struttura del contenuto è la stessa in entrambi i formati:

   - pagina iniziale con sommario / indice
   - per ogni slide:
     - numero slide
     - timestamp
     - immagine
     - testo associato

   Nel PDF possono esserci pagine aggiuntive di continuazione se il testo della slide è troppo lungo per stare nella stessa pagina dell’immagine.

   ### Ruolo nella pipeline

   Questo modulo arriva per ultimo.

   La sequenza completa è:

   - `Screenshot_grabber.py` produce slide e `slides.csv`
   - `export_for_llm.py` costruisce i chunk
   - l’LLM corregge i chunk
   - `import_corrected_for_pdf_docx.py` ricompone il JSON finale
   - `slides_and_texts_to_pdf.py` rende il risultato in PDF e DOCX

   Quindi il suo ruolo non è “capire” nulla di nuovo, ma presentare ordinatamente ciò che a monte è già stato deciso.

   ### Logica operativa generale

   Il flusso del modulo è questo:

   1. parse argomenti CLI
   2. verifica esistenza di directory, CSV e JSON
   3. legge il CSV con `pandas`
   4. legge il JSON e costruisce la mappa `slide_index -> text`
   5. combina CSV e JSON in una lista ordinata di entry
   6. costruisce il PDF
   7. costruisce il DOCX
   8. stampa i path finali creati

   L’idea è semplice: creare una rappresentazione intermedia unica delle slide e poi usarla per due renderer diversi.

   ### Lettura del JSON finale

   La funzione `load_slide_texts_json()` legge il file JSON finale e costruisce una mappa Python:

   ```text
   slide_index -> text
   ```

   Durante questa fase il modulo controlla che:

   - la root JSON sia un oggetto
   - esista la chiave `slides`
   - `slides` sia una lista
   - ogni elemento abbia `slide_index`
   - non ci siano slide duplicate nel JSON

   Il testo viene passato a `clean_final_text()`, che fa una pulizia conservativa:

   - rimuove BOM
   - normalizza newline
   - pulisce spazi multipli
   - conserva i paragrafi
   - sistema piccoli difetti di spaziatura attorno alla punteggiatura

   Questa pulizia è volutamente leggera. Non cerca di riscrivere il testo, ma solo di evitare che piccoli artefatti formali peggiorino la resa tipografica.

   ### Lettura del CSV e costruzione delle entry

   La funzione `build_entries_from_csv_and_json()` prende il DataFrame del CSV e la mappa testo.

   Il CSV deve contenere almeno queste colonne:

   - `slide_index`
   - `timestamp_sec`
   - `filename`

   Le righe vengono ordinate per `timestamp_sec`, poi convertite in una lista di entry con:

   - indice slide
   - tempo inizio slide
   - tempo fine slide
   - filename immagine
   - testo associato

   Il tempo di fine viene calcolato come timestamp della slide successiva; per l’ultima slide viene usato `inf`.

   Questo valore di `slide_end` in questo modulo non è essenziale per il rendering attuale, ma rende la struttura dati più completa e coerente.

   ### Struttura intermedia `entries`

   Ogni slide viene convertita in un dizionario con campi tipo:

   - `slide_index`
   - `slide_start`
   - `slide_end`
   - `filename`
   - `text`

   Questa struttura è il vero cuore del modulo.

   Una volta costruita, il renderer PDF e il renderer DOCX non hanno più bisogno di sapere nulla su CSV o JSON: lavorano entrambi solo su `entries`.

   ### Rendering PDF

   La funzione `build_pdf()` genera un PDF in orientamento orizzontale A4 usando ReportLab.

   Il PDF ha questa struttura:

   1. una o più pagine iniziali di sommario
   2. una pagina per ciascuna slide con immagine + testo
   3. eventuali pagine aggiuntive di continuazione testo

   #### Sommario PDF

   Le prime pagine vengono generate da `draw_summary_pages()`.

   Il sommario contiene:

   - titolo del documento
   - numero totale di slide
   - indice con:
     - numero slide
     - timestamp formattato
     - filename immagine

   Se le slide sono molte, il sommario continua su più pagine.

   #### Pagina slide PDF

   La pagina principale di ogni slide viene costruita da `draw_slide_page()`.

   Contiene:

   - header con numero slide e timestamp
   - footer con numero pagina
   - immagine slide nella parte superiore
   - testo nella parte inferiore

   La pagina usa un layout molto semplice e robusto:

   - margini fissi
   - area immagine circa al 70% dell’altezza utile
   - testo sotto l’immagine

   #### Adattamento immagine nel PDF

   L’immagine non viene stirata arbitrariamente. La funzione `fit_image_in_box()` calcola una scala che la faccia stare dentro il box disponibile mantenendo le proporzioni.

   Quindi:

   - se l’immagine è larga, viene ridotta
   - se è alta, viene adattata in altezza
   - il rapporto d’aspetto resta corretto

   Questo è importante perché evita PDF con slide deformate.

   #### Wrapping del testo PDF

   Il testo nel PDF viene spezzato in righe da `wrap_text_to_width()`.

   La funzione:

   - separa per paragrafi
   - misura la larghezza del testo con `stringWidth()`
   - costruisce righe che non superino la larghezza disponibile
   - se una parola singola è troppo lunga, la spezza a livello carattere

   È una gestione manuale del wrapping, necessaria perché qui il rendering è basso livello e non delegato a un motore di layout avanzato.

   #### Continuazione testo PDF

   Se il testo non entra tutto nella pagina principale della slide, la parte restante viene messa in una o più pagine successive tramite `draw_text_continuation_page()`.

   Queste pagine hanno:

   - header con numero slide
   - dicitura “Continuazione testo”
   - footer con numero pagina
   - solo testo, senza immagine

   Questo rende il modulo adatto anche a slide molto dense, senza troncare contenuto.

   #### Gestione immagini mancanti nel PDF

   Se il file immagine non esiste, il modulo non fallisce subito durante il rendering della singola pagina. Inserisce invece un messaggio visibile del tipo:

   - `[Immagine non trovata: ...]`

   È una scelta pratica: permette comunque di ottenere il documento, segnalando chiaramente il problema.

   ### Rendering DOCX

   La funzione `build_docx()` genera un documento Word usando `python-docx`.

   La logica del DOCX è più semplice del PDF:

   1. imposta la pagina in orizzontale
   2. aggiunge un sommario iniziale
   3. per ogni slide aggiunge:
      - titolo slide
      - immagine
      - testo
      - page break

   In altre parole, il DOCX privilegia semplicità e leggibilità, non una micro-impaginazione fine come nel PDF.

   #### Orientamento e margini DOCX

   La funzione `set_landscape()` modifica la prima sezione del documento:

   - orientamento landscape
   - margini relativamente stretti

   Questo aumenta lo spazio utile per slide e testo.

   #### Sommario DOCX

   La funzione `add_docx_summary()` costruisce una prima sezione con:

   - titolo del documento
   - numero slide
   - indice con slide, timestamp e filename
   - page break finale

   È l’equivalente del sommario PDF, ma in una forma più semplice.

   #### Blocco slide DOCX

   Ogni slide viene aggiunta con `add_slide_block_docx()`.

   Il blocco contiene:

   - heading `Slide N`
   - immagine ridimensionata in larghezza
   - paragrafo con il testo
   - page break

   Se l’immagine non esiste o non è caricabile, il modulo inserisce una riga esplicita di errore invece di interrompersi brutalmente.

   #### Calcolo larghezza immagine DOCX

   La larghezza massima dell’immagine viene calcolata da `get_usable_width_inches()`, in base alle dimensioni realmente disponibili nella pagina.

   Poi `build_docx()` usa quasi tutta la larghezza utile, con un piccolo margine di sicurezza.

   Questo permette di avere immagini grandi e leggibili senza dover hardcodare misure troppo rigide.

   ### Gestione del testo mancante

   Se una slide non ha testo associato oppure il testo è vuoto, il modulo usa un placeholder:

   - `[Nessun testo associato a questa slide]`

   Questo succede sia nel PDF sia nel DOCX.

   È una scelta corretta, perché preserva il numero di slide e rende visibile che quella slide esiste ma non ha contenuto testuale utile.

   ### Utility di testo e tempo

   Il modulo contiene alcune utility piccole ma importanti.

   #### `seconds_to_hms()`

   Converte i secondi in formato `HH:MM:SS` per mostrare timestamp leggibili nei sommari e negli header.

   #### `normalize_whitespace()`

   Comprime gli spazi multipli e fa pulizia di base sulle stringhe.

   #### `clean_final_text()`

   È la funzione chiave di rifinitura del testo già corretto. Non prova a reinterpretare nulla, ma migliora l’aspetto finale:

   - mantiene i paragrafi
   - normalizza righe e spazi
   - pulisce piccole anomalie prima del rendering

   ### Funzioni principali

   Le funzioni più importanti del modulo sono:

   - `load_slide_texts_json()`
     Legge e valida il JSON finale con i testi per slide.
   - `build_entries_from_csv_and_json()`
     Unisce CSV e JSON in una lista canonica di entry.
   - `wrap_text_to_width()`
     Costruisce il wrapping del testo per il PDF.
   - `fit_image_in_box()`
     Adatta l’immagine allo spazio disponibile nel PDF.
   - `draw_summary_pages()`
     Disegna il sommario iniziale del PDF.
   - `draw_slide_page()`
     Disegna la pagina principale di una slide nel PDF.
   - `draw_text_continuation_page()`
     Disegna le eventuali pagine di continuazione testo nel PDF.
   - `build_pdf()`
     Coordina l’intera generazione del PDF.
   - `set_landscape()`
     Imposta il documento Word in orizzontale.
   - `add_docx_summary()`
     Costruisce il sommario del DOCX.
   - `add_slide_block_docx()`
     Aggiunge una slide con immagine e testo nel DOCX.
   - `build_docx()`
     Coordina la generazione del DOCX.
   - `main()`
     Valida input, costruisce le entry e lancia entrambi i renderer.

   ### Dipendenze principali

   Il modulo usa:

   - `pandas` per leggere il CSV
   - `Pillow` per leggere dimensioni immagini
   - `reportlab` per generare il PDF
   - `python-docx` per generare il DOCX
   - librerie standard Python per parsing argomenti, regex, JSON e path

   È quindi il modulo con le dipendenze più “pesanti” della pipeline, cosa normale per un renderer documentale.

   ### Punti forti del modulo

   I punti migliori sono questi:

   - separazione pulita tra fase dati e fase rendering
   - output doppio PDF + DOCX dallo stesso dataset intermedio
   - layout PDF semplice ma robusto
   - supporto a testi lunghi con pagine di continuazione
   - gestione conservativa del testo finale
   - fallback espliciti se mancano immagini o testo

   In pratica è un buon modulo finale: non pretende di essere intelligente, ma è abbastanza robusto da produrre documenti leggibili in molti casi reali.

   ### Limiti pratici

   I limiti sono soprattutto di impaginazione, non di logica.

   Per esempio:

   - il PDF usa un layout fisso, non adattivo in modo sofisticato
   - il DOCX ha una resa più semplice e meno controllata rispetto al PDF
   - non ci sono stili tipografici avanzati
   - non c’è una vera gestione semantica dei paragrafi oltre alla pulizia base
   - se CSV e JSON non corrispondono bene, il modulo non ricostruisce coerenza: si limita a usare ciò che trova

   Inoltre il renderer usa il testo così com’è arrivato dalla fase precedente. Se a monte il testo è mediocre, qui non viene salvato da nessuna post-elaborazione “intelligente”.

   ### Riassunto rapido

   In una frase: `slides_and_texts_to_pdf.py` prende immagini slide, timeline CSV e testo finale per slide, li unisce in una struttura unica e genera i due documenti finali della pipeline, uno in PDF e uno in DOCX, entrambi pensati per la consultazione umana.

   

   ------

   ## Riepilogo ultra rapido

   In forma compatta, lo script fa questo:

   1. scarica video e sottotitoli
   2. estrae le slide
   3. segmenta la trascrizione per slide/chunk
   4. corregge i chunk via LLM
   5. ricompone tutto in JSON
   6. genera PDF e DOCX finali
