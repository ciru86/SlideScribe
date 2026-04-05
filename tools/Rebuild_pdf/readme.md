Sposta in questa cartella le cartelle delle lezioni che contengono i docx da aggiornare alla nuova pipeline, poi fai partire lo script con 

Per tutte le sottocartelle
```bash
python3 tools/Rebuild_pdf/rebuild_legacy_documents.py
```

Per una sola sottocartella:
```bash
python3 tools/Rebuild_pdf/rebuild_legacy_documents.py --folder "Lezione Roberto"
```

Per una sola sottocartella con URL di Youtube
```bash
python3 tools/Rebuild_pdf/rebuild_legacy_documents.py --youtube_url LINK --folder "Lezione Roberto"
```