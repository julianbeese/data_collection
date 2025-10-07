# Brexit-Klassifizierung für UK Parlamentsreden

## Übersicht

Dieses System klassifiziert UK Parlamentsreden nach Brexit-Bezug mittels einer Kombination aus Keyword-Analyse und LLM-Analyse (Gemini 2.0 Flash).

## Voraussetzungen

1. **Gemini API Key** von Google AI Studio holen: https://aistudio.google.com/apikey

2. **Python-Pakete installieren:**
```bash
pip install google-generativeai duckdb
```

3. **Datenbank vorbereitet:** Die `debates.duckdb` muss existieren (erstellt durch `parse_debates.py`)

## Verwendung

### 1. API Key setzen

```bash
export GEMINI_API_KEY='your-api-key-here'
```

### 2. Klassifizierung starten

```bash
python3 classify_brexit.py
```

## Wie es funktioniert

### Schritt 1: Keyword-Analyse (30% Gewichtung)

Das System durchsucht die ersten 5 Redebeiträge jeder Debatte nach 40+ Brexit-relevanten Keywords:

**Direkte Keywords** (höhere Gewichtung):
- brexit, referendum, article 50, leave campaign, withdrawal agreement, etc.

**Indirekte Keywords** (niedrigere Gewichtung):
- european union, sovereignty, customs union, immigration control, etc.

**Confidence-Berechnung:**
- Direkte Keywords: max 0.7 (je 0.3 pro Keyword)
- Indirekte Keywords: max 0.3 (je 0.05 pro Keyword)
- Gesamt: max 1.0

**Wenn keine Keywords gefunden:** Debatte wird übersprungen (kein Brexit-Bezug)

### Schritt 2: LLM-Analyse (70% Gewichtung)

Wenn Keywords gefunden wurden:
- Die ersten 5 Reden (max 8000 Zeichen) werden an Gemini 2.0 Flash gesendet
- Der Prompt fragt nach Brexit-Bezug mit Kontext zu:
  - Datum der Debatte
  - Debattenname
  - Gefundene Keywords
  - Redetexte

**LLM gibt zurück:**
- `has_brexit_relation`: true/false
- `confidence`: 0.0-1.0
- `reasoning`: Ein-Satz-Erklärung

### Schritt 3: Kombinierte Entscheidung

```
final_confidence = (0.3 × keyword_confidence) + (0.7 × llm_confidence)
brexit_related = final_confidence > 0.5
```

**Alle Reden derselben Debatte** (gleicher Name + Datum) erhalten die gleiche Klassifizierung.

## Output

### Neue Datenbank: `debates_brexit_classified.duckdb`

Kopie der Originaldatenbank mit zusätzlichen Spalten in der `speeches`-Tabelle:

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| `brexit_related` | BOOLEAN | Finale Entscheidung |
| `brexit_confidence` | FLOAT | Kombinierte Confidence (0-1) |
| `brexit_keyword_confidence` | FLOAT | Keyword-Score |
| `brexit_llm_confidence` | FLOAT | LLM-Score |
| `brexit_keywords_found` | VARCHAR | Gefundene Keywords (erste 10) |
| `brexit_llm_reasoning` | VARCHAR | LLM-Begründung |

## Beispiel-Abfragen

```sql
-- Alle Brexit-bezogenen Reden
SELECT * FROM speeches WHERE brexit_related = TRUE;

-- Anzahl Brexit-Reden pro Jahr
SELECT
    YEAR(d.date) as year,
    COUNT(*) as brexit_speeches
FROM speeches s
JOIN debates d ON s.debate_id = d.debate_id
WHERE s.brexit_related = TRUE
GROUP BY year
ORDER BY year;

-- Top Redner zu Brexit
SELECT
    speaker_name,
    COUNT(*) as brexit_speech_count
FROM speeches
WHERE brexit_related = TRUE AND speaker_name IS NOT NULL
GROUP BY speaker_name
ORDER BY brexit_speech_count DESC
LIMIT 10;

-- Debatten mit höchster Brexit-Confidence
SELECT DISTINCT
    d.date,
    d.major_heading_text,
    s.brexit_confidence,
    s.brexit_llm_reasoning
FROM speeches s
JOIN debates d ON s.debate_id = d.debate_id
WHERE s.brexit_related = TRUE
ORDER BY s.brexit_confidence DESC
LIMIT 20;
```

## Keywords-Liste

### Direkte Brexit-Keywords (13)
- brexit
- leave campaign / remain campaign
- article 50
- referendum / eu referendum / european referendum
- leave the eu / leaving the eu
- exit from europe
- withdrawal agreement
- divorce bill
- transition period
- hard brexit / soft brexit / no-deal brexit

### Indirekte Keywords (28)
- european union / european community / eu membership
- brussels / strasbourg
- european commission / european parliament
- eurozone / single market / customs union
- free movement / schengen
- eu law / eu regulation / eu directive
- eu budget / eu contribution
- sovereignty / independence / british sovereignty
- take back control
- immigration control / border control
- trade agreement / trade deal / wto
- northern ireland protocol / backstop / irish border

## Kosten-Schätzung

Bei ~10.000 Debatten mit Keywords:
- Gemini 2.0 Flash: ~$0.001 pro Anfrage
- Geschätzte Kosten: ~$10-20

## Performance

- Keyword-Analyse: Sofort
- LLM-Analyse: ~1-2 Sekunden pro Debatte
- Gesamtzeit: Abhängig von Anzahl Debatten mit Keywords

## Fehlerbehandlung

- API-Fehler werden geloggt, Debatte erhält `brexit_related=FALSE`
- JSON-Parse-Fehler: Fallback auf `FALSE`
- Rate Limits: System pausiert automatisch bei API-Fehlern
