# Archive - Veraltete Dateien

## 📁 Struktur

### `old_scripts/` - Veraltete Skripte
- **`database_chunking.py`** - Alte Chunking-Methode (ersetzt durch simple_database_chunking.py)
- **`smart_chunking.py`** - Experimentelle Chunking-Methode
- **`simple_annotation.py`** - Lokale Annotation (ersetzt durch Railway-Version)
- **`simple_export.py`** - Alte Export-Methode
- **`migrate_to_railway.py`** - Erste Migration (ersetzt durch migrate_optimized.py)
- **`migrate_local_to_railway.py`** - Zweite Migration (ersetzt durch migrate_optimized.py)
- **`start_annotation_db.py`** - Lokale DB-Start (ersetzt durch Railway)
- **`streamlit_annotation_db.py`** - Lokale Annotation App (ersetzt durch Railway-Version)

### `old_data/` - Alte Daten
- **`annotation_template_semantic.csv`** - Altes Template
- **`speech_chunks_semantic.json`** - Alte Chunk-Daten

### `old_exports/` - Alte Exports
- **`railway_export/`** - Erste CSV-Exports
- **`railway_export_optimized/`** - Optimierte CSV-Exports

## 🔄 Migration History

1. **Erste Version**: Lokale Annotation mit SQLite
2. **Zweite Version**: Railway Migration (langsam)
3. **Dritte Version**: Optimierte Migration (15k Batches)
4. **Aktuelle Version**: Nur annotierte Chunks + Sync zurück

## ⚠️ Hinweise

- Diese Dateien sind **nicht mehr aktiv**
- **Nicht löschen** - für Referenz und Rollback
- **Aktuelle Skripte** sind in `scripts/current/`
