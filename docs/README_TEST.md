# Brexit-Klassifizierung Test - Januar 2016

## Zweck

Dieses Test-Script testet die Brexit-Klassifizierung auf einem kleineren Datensatz (Januar 2016), um:
- Die Funktionalität zu verifizieren
- Die Qualität der Ergebnisse zu prüfen
- API-Kosten zu minimieren während der Entwicklung
- Schnelles Feedback zu erhalten

## Warum Januar 2016?

Januar 2016 ist ein interessanter Testmonat, weil:
- Das EU-Referendum war am **23. Juni 2016**
- Januar 2016 war **vor** dem Referendum
- Es gab bereits intensive Debatten über ein mögliches Referendum
- David Cameron führte EU-Reform-Verhandlungen

**Erwartung:** Einige Debatten sollten Brexit-Bezug haben (EU-Referendum-Diskussionen), aber viele auch nicht (normale parlamentarische Arbeit).

## Verwendung

### 1. Voraussetzungen

```bash
# API Key setzen
export GEMINI_API_KEY='your-key-here'

# Packages installieren (falls noch nicht geschehen)
pip install google-generativeai duckdb
```

### 2. Test ausführen

```bash
python3 test_brexit_classification.py
```

### 3. Output

Das Script erstellt:
- **debates_brexit_test_jan2016.duckdb** - Testdatenbank mit Klassifizierungen
- Detaillierte Konsolenausgabe mit:
  - Fortschritt jeder Debatte
  - Keyword-Analyse-Ergebnisse
  - LLM-Analyse-Ergebnisse
  - Finale Klassifizierung
  - Zusammenfassungs-Tabelle

## Beispiel-Output

```
======================================================================
BREXIT-KLASSIFIZIERUNG TEST - JANUAR 2016
======================================================================

✓ Gemini API Key gefunden
✓ Verwende Modell: gemini-2.0-flash-exp

Öffne Quelldatenbank: debates.duckdb
Erstelle Test-Datenbank: debates_brexit_test_jan2016.duckdb
  Kopiere Januar 2016 debates...
  Kopiere Januar 2016 topics...
  Kopiere Januar 2016 speeches...
  Füge Brexit-Klassifizierungsspalten hinzu...

Gefunden: 45 Debatten in Januar 2016

Starte Klassifizierung...

[1/45] 2016-01-04 - BUSINESS, INNOVATION AND SKILLS
  Keywords: 3 gefunden, Confidence: 0.15
    → european union, eu membership, sovereignty
  Analysiere mit Gemini...
  LLM: False, Confidence: 0.30
  Reasoning: Discussion about business policy, not Brexit-specific
  ✓ Final: Brexit-Bezug = False, Confidence = 0.26

[2/45] 2016-01-05 - EUROPEAN UNION REFERENDUM
  Keywords: 8 gefunden, Confidence: 0.70
    → referendum, eu referendum, european union, leave the eu, sovereignty
  Analysiere mit Gemini...
  LLM: True, Confidence: 0.95
  Reasoning: Direct debate about the upcoming EU referendum
  ✓ Final: Brexit-Bezug = True, Confidence = 0.88

...
```

## Interpretation der Ergebnisse

### Confidence-Werte

- **0.0 - 0.3**: Kein oder sehr schwacher Brexit-Bezug
- **0.3 - 0.5**: Indirekter Bezug (EU-Themen, aber nicht Brexit-spezifisch)
- **0.5 - 0.7**: Wahrscheinlicher Brexit-Bezug
- **0.7 - 1.0**: Starker, direkter Brexit-Bezug

### Finale Entscheidung

- **Brexit-Bezug = TRUE**: Wenn `combined_confidence > 0.5`
- Berechnung: `0.3 × keyword_conf + 0.7 × llm_conf`

## Qualitätsprüfung

Nach dem Test solltest du:

1. **Ergebnistabelle prüfen**
   - Sehen die Brexit-positiven Debatten sinnvoll aus?
   - Sind offensichtliche Brexit-Debatten erkannt?

2. **Reasoning lesen**
   - Ist die LLM-Begründung nachvollziehbar?
   - Macht die Klassifizierung Sinn?

3. **Falsch-Positive prüfen**
   - Gibt es Debatten mit Brexit-Label, die nicht Brexit-bezogen sind?

4. **Falsch-Negative prüfen**
   - Fehlen offensichtliche Brexit-Debatten?

5. **Datenbank inspizieren**
   ```sql
   duckdb debates_brexit_test_jan2016.duckdb

   -- Alle Brexit-Debatten
   SELECT DISTINCT
       d.date,
       d.major_heading_text,
       s.brexit_confidence,
       s.brexit_llm_reasoning
   FROM speeches s
   JOIN debates d ON s.debate_id = d.debate_id
   WHERE s.brexit_related = TRUE
   ORDER BY s.brexit_confidence DESC;

   -- Grenzfälle (Confidence um 0.5)
   SELECT DISTINCT
       d.date,
       d.major_heading_text,
       s.brexit_confidence,
       s.brexit_keywords_found
   FROM speeches s
   JOIN debates d ON s.debate_id = d.debate_id
   WHERE s.brexit_confidence BETWEEN 0.4 AND 0.6
   ORDER BY s.brexit_confidence DESC;
   ```

## Erwartete Kosten

Bei ~45 Debatten und angenommen 20-30 haben Keywords:
- Gemini 2.0 Flash API Calls: ~20-30
- Kosten: ~$0.02-0.03 (sehr günstig für Tests)

## Nächste Schritte

Wenn der Test erfolgreich ist:

1. **Vollständige Klassifizierung**
   ```bash
   python3 classify_brexit.py
   ```
   Klassifiziert alle ~10,000+ Debatten (2012-2022)

2. **Parameter anpassen**
   Falls nötig, kannst du in `classify_brexit.py` anpassen:
   - Keyword-Listen erweitern/reduzieren
   - Gewichtung ändern (z.B. 40/60 statt 30/70)
   - LLM-Prompt verfeinern

3. **Weitere Tests**
   Du könntest auch andere Zeiträume testen:
   - **Juni 2016** (Referendum-Monat)
   - **März 2017** (Article 50)
   - **Januar 2020** (Brexit vollzogen)
