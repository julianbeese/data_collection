# Brexit-Reklassifizierung mit Gemini 2.5-Flash

## Übersicht

Dieses Script analysiert **alle Reden** in der gefilterten Datenbank `debates_brexit_filtered_min20words.duckdb` erneut mit Gemini 2.5-Flash auf Brexit-Bezug. Im Gegensatz zum ursprünglichen `classify_brexit.py` wird:

- **Jede einzelne Rede** analysiert (nicht nur pro Debatte)
- **Keine Keyword-Vorfilterung** durchgeführt
- Nur **Brexit-bezogene Reden** werden in die neue Datenbank übernommen
- **Vereinfachtes Schema** mit nur den neuen Klassifizierungsergebnissen

## Unterschied zu classify_brexit.py

| Feature | classify_brexit.py | reclassify_brexit_gemini.py |
|---------|-------------------|----------------------------|
| Analysiert | Pro Debatte (erste 5 Reden) | Jede einzelne Rede |
| Keyword-Filter | Ja (überspringt bei 0 Keywords) | Nein |
| Gemini-Modell | gemini-2.5-flash | gemini-2.5-flash |
| Output | Alle Reden + Klassifizierung | Nur Brexit-Reden |
| Spalten | 6 Klassifizierungsspalten | 3 Klassifizierungsspalten |
| Ursprungsdatenbank | Unverändert | Unverändert |

## Voraussetzungen

1. **Gemini API Key** von Google AI Studio: https://aistudio.google.com/apikey

2. **Python-Pakete installieren:**
```bash
pip install google-generativeai duckdb python-dotenv
```

3. **Quelldatenbank:** `debates_brexit_filtered_min20words.duckdb` muss im Hauptverzeichnis vorhanden sein

## Verwendung

### 1. API Key setzen

**Windows (PowerShell):**
```powershell
$env:GEMINI_API_KEY = "your-api-key-here"
```

**Linux/Mac:**
```bash
export GEMINI_API_KEY='your-api-key-here'
```

Oder erstelle eine `.env` Datei im Hauptverzeichnis:
```
GEMINI_API_KEY=your-api-key-here
```

### 2. Script starten

```bash
cd scripts
python reclassify_brexit_gemini.py
```

## Wie es funktioniert

### Analyse-Prozess

1. **Lese alle Reden** aus `debates_brexit_filtered_min20words.duckdb`
2. **Für jede Rede:**
   - Sende Rede-Text (max 6000 Zeichen) an Gemini 2.5-Flash
   - Gemini analysiert auf Brexit-Bezug
   - Gibt zurück: `has_brexit_relation` (bool), `confidence` (0-1), `reasoning` (ein Satz)
3. **Nur Brexit-bezogene Reden** (`has_brexit_relation = true`) werden gespeichert
4. **Zugehörige Debates und Topics** werden automatisch mitkopiert

### Rate Limiting

- **10 Requests/Minute** (6 Sekunden Pause zwischen API-Calls)
- Bei Rate Limit Errors: Automatisches Retry mit exponential backoff (6s, 12s, 24s, 48s, 96s)
- Max 5 Retry-Versuche

### Cost Control

- **Kosten-Limit:** $20.00 (konfigurierbar)
- **Pricing:** 
  - Input: $0.075 per 1M tokens
  - Output: $0.30 per 1M tokens
- Bei Erreichen des Limits: Automatischer Stop und Speicherung bisheriger Ergebnisse

## Output

### Neue Datenbank: `debates_brexit_gemini_classified.duckdb`

Enthält nur Brexit-relevante Reden mit vereinfachtem Schema:

#### Tabelle: `speeches`

**Original-Spalten:**
- `speech_id`, `topic_id`, `debate_id`
- `speaker_name`, `person_id`, `speaker_office`
- `speech_type`, `oral_qnum`
- `colnum`, `time`, `url`
- `speech_text`, `paragraph_count`

**Neue Klassifizierungs-Spalten:**

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| `brexit_related` | BOOLEAN | Immer `TRUE` (nur Brexit-Reden werden gespeichert) |
| `brexit_confidence` | FLOAT | Confidence-Score von Gemini (0.0-1.0) |
| `brexit_reasoning` | VARCHAR | Ein-Satz-Begründung von Gemini |

#### Tabellen: `debates` und `topics`

Unverändert, enthalten nur Einträge, die zu gespeicherten Reden gehören.

## Beispiel-Abfragen

```sql
-- Alle Reden mit hoher Confidence
SELECT 
    speaker_name,
    speech_text,
    brexit_confidence,
    brexit_reasoning
FROM speeches
WHERE brexit_confidence > 0.8
ORDER BY brexit_confidence DESC;

-- Durchschnittliche Confidence pro Debatte
SELECT 
    d.date,
    d.major_heading_text,
    COUNT(*) as speech_count,
    AVG(s.brexit_confidence) as avg_confidence
FROM speeches s
JOIN debates d ON s.debate_id = d.debate_id
GROUP BY d.date, d.major_heading_text
ORDER BY avg_confidence DESC
LIMIT 20;

-- Redner mit meisten Brexit-Reden
SELECT 
    speaker_name,
    COUNT(*) as speech_count,
    AVG(brexit_confidence) as avg_confidence
FROM speeches
WHERE speaker_name IS NOT NULL
GROUP BY speaker_name
ORDER BY speech_count DESC
LIMIT 20;

-- Zeitliche Verteilung
SELECT 
    YEAR(d.date) as year,
    MONTH(d.date) as month,
    COUNT(*) as brexit_speeches
FROM speeches s
JOIN debates d ON s.debate_id = d.debate_id
GROUP BY year, month
ORDER BY year, month;
```

## Kosten-Schätzung

Bei angenommenen Durchschnittswerten:
- **Durchschnittliche Rede:** ~800 Tokens Input, ~50 Tokens Output
- **Pro Rede:** ~$0.00008 
- **10.000 Reden:** ~$0.80
- **50.000 Reden:** ~$4.00
- **100.000 Reden:** ~$8.00

Tatsächliche Kosten werden während der Ausführung angezeigt.

## Performance

- **Analyse-Geschwindigkeit:** ~10 Reden/Minute (Rate Limit)
- **Geschätzte Zeit:**
  - 1.000 Reden: ~1.7 Stunden
  - 10.000 Reden: ~17 Stunden
  - 50.000 Reden: ~3.5 Tage

**Tipp:** Script kann unterbrochen und neu gestartet werden. Bereits klassifizierte Reden werden nicht erneut analysiert (implementieren falls gewünscht).

## Progress Tracking

Das Script zeigt während der Ausführung:
- Anzahl verarbeitete Reden
- Brexit vs. Non-Brexit Verhältnis
- Aktuelle Kosten
- Geschätzte Restzeit

Zwischenspeicherung erfolgt alle 100 Reden automatisch.

## Fehlerbehandlung

- **API-Fehler:** Werden geloggt, Rede wird als Non-Brexit behandelt
- **JSON-Parse-Fehler:** Fallback auf `has_brexit_relation=FALSE`
- **Rate Limit:** Automatisches Retry mit exponential backoff
- **Cost Limit erreicht:** Automatischer Stop mit Speicherung aller bisherigen Ergebnisse

## Fortsetzen nach Abbruch

Da alle Non-Brexit Reden verworfen werden, kann das Script bei Abbruch nicht automatisch fortsetzen. Es müsste von vorne starten. Eine Resume-Funktionalität könnte implementiert werden, indem bereits verarbeitete `speech_id`s in einer separaten Tabelle gespeichert werden.

## Empfehlungen

1. **Test-Run:** Starte zunächst mit einem Kosten-Limit von $1-2, um die Datenqualität zu prüfen
2. **Monitoring:** Überwache die ersten 100 Reden, um sicherzustellen, dass die Klassifizierung korrekt funktioniert
3. **Backup:** Die Ursprungsdatenbank bleibt unverändert, aber erstelle ein Backup falls nötig
4. **API Key:** Verwende eine `.env` Datei statt direkt im Terminal zu setzen (sicherer)

## Konfiguration

Im Script anpassbar (Zeilen 19-24):

```python
DB_FILE = "debates_brexit_filtered_min20words.duckdb"  # Quelldatenbank
OUTPUT_DB = "debates_brexit_gemini_classified.duckdb"  # Zieldatenbank
GEMINI_MODEL = "gemini-2.5-flash"                      # Gemini-Modell
COST_LIMIT = 20.00                                     # Maximale Kosten in USD
REQUEST_DELAY = 6.0                                    # Sekunden zwischen API-Calls
```

## Troubleshooting

### "GEMINI_API_KEY nicht gesetzt"
- Setze die Umgebungsvariable oder erstelle eine `.env` Datei

### "Datenbank nicht gefunden"
- Stelle sicher, dass du das Script aus dem `scripts/` Ordner ausführst
- Oder passe den Pfad `DB_FILE` im Script an

### "Rate Limit erreicht"
- Das Script wartet automatisch und versucht es erneut
- Bei anhaltenden Problemen: Erhöhe `REQUEST_DELAY` auf 10+ Sekunden

### "JSON nicht geparsed"
- Gelegentliche API-Antworten sind nicht im erwarteten Format
- Diese Reden werden als Non-Brexit klassifiziert und nicht gespeichert

