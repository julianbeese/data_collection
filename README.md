# Brexit Data Collection Project

Ein Python-Projekt zur Sammlung, Verarbeitung und Analyse von Brexit-bezogenen Parlamentsdebatten aus dem britischen Unterhaus.

## Projektstruktur

```
data_collection/
├── scripts/                    # Python-Skripte
│   ├── scrape_debates.py      # Scraper für XML-Dateien von TheyWorkForYou
│   ├── parse_debates.py       # Parser für XML-Dateien in DuckDB
│   ├── classify_brexit.py     # Brexit-Klassifizierung mit Keywords und LLM
│   ├── filter_brexit_speeches.py  # Filtert Brexit-relevante Reden
│   └── query_debates.py       # Beispiel-Abfragen für die Datenbank
├── data/                       # Daten
│   ├── raw/                   # Rohdaten (XML-Dateien)
│   └── processed/             # Verarbeitete Daten (DuckDB-Dateien)
│       ├── debates.duckdb
│       ├── debates_brexit_classified.duckdb
│       └── debates_brexit_filtered.duckdb
├── tests/                     # Test-Dateien
│   └── test_brexit_classification.py
├── docs/                      # Dokumentation
│   ├── README_BREXIT_CLASSIFICATION.md
│   └── README_TEST.md
├── requirements.txt           # Python-Abhängigkeiten
└── venv/                     # Virtuelle Umgebung
```

## Workflow

1. **Daten sammeln**: `scripts/scrape_debates.py` lädt XML-Dateien von TheyWorkForYou herunter
2. **Daten parsen**: `scripts/parse_debates.py` konvertiert XML in DuckDB-Datenbank
3. **Brexit klassifizieren**: `scripts/classify_brexit.py` identifiziert Brexit-relevante Reden
4. **Daten filtern**: `scripts/filter_brexit_speeches.py` erstellt gefilterte Datenbank
5. **Daten abfragen**: `scripts/query_debates.py` zeigt Beispiel-Abfragen

## Installation

```bash
# Virtuelle Umgebung aktivieren
source venv/bin/activate

# Abhängigkeiten installieren
pip install -r requirements.txt
```

## Verwendung

### 1. Daten sammeln
```bash
cd scripts
python scrape_debates.py
```

### 2. Daten parsen
```bash
python parse_debates.py
```

### 3. Brexit klassifizieren
```bash
python classify_brexit.py
```

### 4. Brexit-Daten filtern
```bash
python filter_brexit_speeches.py
```

### 5. Daten abfragen
```bash
python query_debates.py
```

## Datenbank-Schema

- **debates**: Debatten-Metadaten
- **topics**: Themen und Überschriften
- **speeches**: Einzelne Reden mit Text und Metadaten
- **brexit_classification**: Brexit-Klassifizierungsergebnisse

## Konfiguration

Für die LLM-Klassifizierung wird eine `.env` Datei mit dem Gemini API-Key benötigt:
```
GEMINI_API_KEY=your_api_key_here
```
