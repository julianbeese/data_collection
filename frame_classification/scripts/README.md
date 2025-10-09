# Frame Classification Scripts

## 📁 Aktuelle Struktur

### `current/` - Aktive Skripte
- **`simple_database_chunking.py`** - Optimiertes Chunking mit Absatz-basierten Einheiten (1300 Zeichen)
- **`migrate_optimized.py`** - Ultra-schnelle Migration zu Railway (15k Batches)
- **`migrate_annotated_only.py`** - Migration nur annotierte Chunks (2k statt 224k)
- **`sync_railway_to_local.py`** - Sync Railway-Annotationen zurück zu lokaler DuckDB
- **`streamlit_annotation_railway.py`** - Railway Annotation App
- **`fine_tuning_pipeline.py`** - ML Pipeline für Fine-Tuning
- **`generate_training_data.py`** - Training Data Generation

### `archive/` - Archivierte Skripte
- **`old_scripts/`** - Veraltete/ersetzte Skripte
- **`old_data/`** - Alte Daten-Templates
- **`old_exports/`** - Alte CSV-Exports

## 🚀 Workflow

### 1. Chunking
```bash
python current/simple_database_chunking.py --input-db ../../data/processed/debates_brexit_classified.duckdb
```

### 2. Migration zu Railway
```bash
# Nur annotierte Chunks (empfohlen)
python current/migrate_annotated_only.py --duckdb ../../data/processed/debates_brexit_chunked.duckdb

# Alle Chunks (bei genügend Speicherplatz)
python current/migrate_optimized.py --duckdb ../../data/processed/debates_brexit_chunked.duckdb
```

### 3. Sync zurück zu lokal
```bash
python current/sync_railway_to_local.py --duckdb ../../data/processed/debates_brexit_chunked.duckdb
```

### 4. Annotation App
```bash
streamlit run current/streamlit_annotation_railway.py
```

## 📊 Performance-Optimierungen

- **Chunking**: Absatz-basiert, 1300 Zeichen, Q&A/Quotes/Listen zusammenhalten
- **Migration**: 15k Batch-Size, execute_values, DuckDB Native CSV
- **Sync**: Nur annotierte Daten, RealDictCursor, Batch-Updates
