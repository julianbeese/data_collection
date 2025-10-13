# LLM-Spalten leeren Script

## Übersicht

Das Script `clear_llm_columns.py` leert alle LLM-bezogenen Spalten in der `brexit_analysis.duckdb` Datenbank, um einen Neustart der LLM-Klassifizierung zu ermöglichen.

## Verwendung

### Voraussetzungen

- Python 3.7+
- DuckDB installiert
- Virtuelle Umgebung aktiviert

### Script ausführen

```bash
# Virtuelle Umgebung aktivieren
source venv/bin/activate

# Script ausführen
python3 scripts/clear_llm_columns.py
```

## Was das Script macht

### 1. Identifiziert LLM-Spalten

Das Script sucht nach folgenden LLM-bezogenen Spalten:

**Hauptklassifizierung:**
- `llm_processed` → `FALSE`
- `llm_classified_brexit` → `FALSE` 
- `llm_confidence_score` → `NULL`
- `llm_reasoning` → `NULL`
- `llm_key_indicators` → `NULL`

**Provider-Informationen:**
- `llm_provider` → `NULL`
- `llm_model` → `NULL`

**Kosten und Performance:**
- `llm_cost_usd` → `NULL`
- `llm_processing_time` → `NULL`
- `llm_input_tokens` → `NULL`
- `llm_output_tokens` → `NULL`

**Fehlerbehandlung:**
- `llm_error` → `NULL`
- `llm_processed_at` → `NULL`

**Legacy-Spalten:**
- `brexit_llm_confidence` → `NULL`
- `brexit_llm_reasoning` → `NULL`
- `brexit_confidence` → `NULL`
- `brexit_related` → `FALSE`

### 2. Leert die Spalten

- Setzt Boolean-Spalten auf `FALSE`
- Setzt numerische und Text-Spalten auf `NULL`
- Aktualisiert alle Zeilen in der `speeches`-Tabelle

### 3. Zeigt Statistiken

- Anzahl der aktualisierten Zeilen
- Verteilung der Werte nach dem Leeren

## Fehlerbehandlung

### Datenbank ist gesperrt

Falls Sie diese Fehlermeldung erhalten:
```
IO Error: Could not set lock on file "...": Conflicting lock is held
```

**Lösungen:**
1. Schließen Sie alle anderen Programme, die die Datenbank verwenden
2. Schließen Sie Cursor und öffnen Sie es erneut
3. Warten Sie einen Moment und versuchen Sie es erneut
4. Prüfen Sie, ob andere Python-Scripts laufen

### Datenbank nicht gefunden

Falls die Datenbank nicht gefunden wird:
```
❌ Datenbank nicht gefunden: /path/to/brexit_analysis.duckdb
```

**Lösungen:**
1. Prüfen Sie, ob die Datenbank existiert
2. Führen Sie zuerst `create_brexit_analysis_db.py` aus
3. Prüfen Sie den Pfad in der Script-Konfiguration

## Nach dem Leeren

Nach erfolgreichem Ausführen des Scripts können Sie:

1. **LLM-Klassifizierung neu starten:**
   ```bash
   python3 scripts/classify_brexit_with_gemini.py
   ```

2. **Test-Klassifizierung durchführen:**
   ```bash
   python3 scripts/test_gemini_classification.py
   ```

3. **Datenbank analysieren:**
   ```bash
   python3 scripts/query_debates.py
   ```

## Sicherheit

- Das Script erstellt automatisch ein Backup vor dem Leeren
- Alle Änderungen werden in einer Transaktion durchgeführt
- Bei Fehlern wird ein Rollback durchgeführt

## Beispiel-Output

```
================================================================================
🧹 LLM-SPALTEN LEEREN SCRIPT
================================================================================
Ziel: /path/to/brexit_analysis.duckdb
Zeit: 2025-10-11 19:49:42
✅ Datenbank gefunden: /path/to/brexit_analysis.duckdb
✅ Datenbankverbindung erfolgreich

🔍 Prüfe existierende LLM-Spalten...
  ✓ Gefunden: llm_processed
  ✓ Gefunden: llm_classified_brexit
  ✓ Gefunden: llm_confidence_score
  ✓ Gefunden: llm_reasoning

🧹 Leere 4 LLM-Spalten...
📝 Führe aus: UPDATE speeches SET llm_processed = FALSE, llm_classified_brexit = FALSE, llm_confidence_score = NULL, llm_reasoning = NULL
✅ 12345 Zeilen aktualisiert
✅ Änderungen gespeichert

📊 Statistiken nach dem Leeren:
  Gesamt Speeches: 12345
  llm_processed = TRUE: 0
  llm_classified_brexit = TRUE: 0

✅ LLM-Spalten erfolgreich geleert!

🎉 Fertig! Sie können jetzt mit der LLM-Klassifizierung neu starten.
💡 Tipp: Führen Sie Ihr Klassifizierungs-Script erneut aus.
🔌 Datenbankverbindung geschlossen
```






