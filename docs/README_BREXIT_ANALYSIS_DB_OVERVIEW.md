# Übersicht: brexit_analysis.duckdb Datenbankstruktur

## Allgemeine Informationen
- **Datenbank**: `brexit_analysis.duckdb`
- **Speicherort**: `data/processed/brexit_analysis.duckdb`
- **Anzahl Tabellen**: 3
- **Gesamtanzahl Datensätze**: 62,061

---

## Tabelle 1: `debates` (4,188 Datensätze)

### Spalten und Datentypen:
| Spalte | Datentyp | Beschreibung |
|--------|----------|--------------|
| `debate_id` | VARCHAR | Eindeutige ID des Debattes |
| `date` | DATE | Datum der Debatte |
| `file_name` | VARCHAR | Name der XML-Datei |
| `major_heading_text` | VARCHAR | Hauptüberschrift der Debatte |
| `colnum` | VARCHAR | Spaltennummer |
| `time` | VARCHAR | Uhrzeit der Debatte |
| `url` | VARCHAR | URL zur Debatte |

### Charakteristika:
- **Zeitraum**: 2012-01-10 bis 2022-12-20
- **Einzigartige Termine**: 1,487
- **Beispiel-Debatten**: "PRIME MINISTER", "Debate on the Address", "Business of the House"

---

## Tabelle 2: `speeches` (40,024 Datensätze)

### Grundlegende Spalten:
| Spalte | Datentyp | Beschreibung |
|--------|----------|--------------|
| `speech_id` | VARCHAR | Eindeutige ID der Rede |
| `topic_id` | VARCHAR | ID des Themas |
| `debate_id` | VARCHAR | ID der zugehörigen Debatte |
| `speaker_name` | VARCHAR | Name des Sprechers |
| `person_id` | VARCHAR | Eindeutige Person-ID |
| `speaker_office` | VARCHAR | Amt des Sprechers |
| `speech_type` | VARCHAR | Art der Rede |
| `oral_qnum` | VARCHAR | Nummer der mündlichen Frage |
| `colnum` | VARCHAR | Spaltennummer |
| `time` | VARCHAR | Uhrzeit der Rede |
| `url` | VARCHAR | URL zur Rede |
| `speech_text` | VARCHAR | Volltext der Rede |
| `paragraph_count` | INTEGER | Anzahl der Absätze |

### Brexit-Klassifikation Spalten:
| Spalte | Datentyp | Beschreibung |
|--------|----------|--------------|
| `brexit_related` | BOOLEAN | Ob die Rede Brexit-bezogen ist |
| `confidence_score` | FLOAT | Vertrauenswert der Klassifikation |
| `lists_found_keywords` | VARCHAR | Gefundene Schlüsselwörter |

### LLM-Verarbeitung Spalten:
| Spalte | Datentyp | Beschreibung |
|--------|----------|--------------|
| `llm_processed` | BOOLEAN | Ob LLM-Verarbeitung stattgefunden hat |
| `llm_classified_brexit` | BOOLEAN | LLM-Brexit-Klassifikation |
| `llm_confidence_score` | FLOAT | LLM-Vertrauenswert |
| `llm_key_indicators` | VARCHAR | LLM-identifizierte Schlüsselindikatoren |
| `llm_provider` | VARCHAR | LLM-Anbieter |
| `llm_model` | VARCHAR | Verwendetes LLM-Modell |
| `llm_cost_usd` | FLOAT | Kosten in USD |
| `llm_processing_time` | FLOAT | Verarbeitungszeit |
| `llm_input_tokens` | INTEGER | Eingabe-Tokens |
| `llm_output_tokens` | INTEGER | Ausgabe-Tokens |
| `llm_error` | VARCHAR | Fehlermeldung |
| `llm_processed_at` | TIMESTAMP | Zeitpunkt der Verarbeitung |
| `llm_reasoning` | VARCHAR | LLM-Begründung |
| `llm_retry_count` | INTEGER | Anzahl Wiederholungen |

### Datencharakteristika:

#### Brexit-Klassifikation:
- **Nicht Brexit-bezogen**: 40,024 (100%)
- **Brexit-bezogen**: 0 (0%)

#### Confidence Score Statistiken:
- **Minimum**: 0.1000
- **Maximum**: 1.0000
- **Durchschnitt**: 0.1781
- **Anzahl mit Score > 0.5**: 761

#### LLM-Verarbeitung:
- **Nicht verarbeitet**: 40,024 (100%)
- **Verarbeitet**: 0 (0%)

#### Sprecher-Statistiken:
- **Einzigartige Sprecher**: 1,042
- **Einzigartige Person IDs**: 1,357
- **Einzigartige Ämter**: 55

#### Text-Statistiken:
- **Zeichen Länge**: 
  - Minimum: 118 Zeichen
  - Maximum: 108,337 Zeichen
  - Durchschnitt: 1,860 Zeichen
- **Absätze**:
  - Minimum: 1 Absatz
  - Maximum: 930 Absätze
  - Durchschnitt: 4.1 Absätze

#### Top 10 Keywords:
1. **brexit**: 7,444 Vorkommen
2. **referendum**: 5,038 Vorkommen
3. **european union, leave the eu**: 1,263 Vorkommen
4. **leave the eu**: 883 Vorkommen
5. **withdrawal agreement**: 853 Vorkommen
6. **european union, referendum**: 808 Vorkommen
7. **independence, referendum**: 785 Vorkommen
8. **article 50**: 717 Vorkommen
9. **brexit, european union**: 709 Vorkommen
10. **brexit, no-deal brexit**: 685 Vorkommen

---

## Tabelle 3: `topics` (17,849 Datensätze)

### Spalten und Datentypen:
| Spalte | Datentyp | Beschreibung |
|--------|----------|--------------|
| `topic_id` | VARCHAR | Eindeutige ID des Themas |
| `debate_id` | VARCHAR | ID der zugehörigen Debatte |
| `minor_heading_text` | VARCHAR | Unterüberschrift/Thema |
| `colnum` | VARCHAR | Spaltennummer |
| `time` | VARCHAR | Uhrzeit |
| `url` | VARCHAR | URL zum Thema |

### Charakteristika:
- **Beispiel-Themen**: "Engagements", "Airport Security (People with Disabilities)", "Welfare Reform Bill (Programme) (No. 3)"

---

## Zusammenfassung

Die `brexit_analysis.duckdb` Datenbank enthält parlamentarische Debatten und Reden aus dem britischen Unterhaus mit Fokus auf Brexit-Themen. Die Datenbank ist strukturiert in drei Haupttabellen:

1. **`debates`**: Metadaten zu parlamentarischen Debatten
2. **`speeches`**: Einzelne Reden mit umfangreicher Brexit-Klassifikation und LLM-Verarbeitung
3. **`topics`**: Themen und Unterüberschriften der Debatten

**Wichtige Erkenntnisse:**
- Alle 40,024 Reden sind aktuell als "nicht Brexit-bezogen" klassifiziert
- Keine LLM-Verarbeitung hat bisher stattgefunden
- Die Datenbank enthält umfangreiche Metadaten für zukünftige Analysen
- Zeitraum der Daten: 2012-2022 (über 10 Jahre parlamentarische Debatten)
- Hohe Anzahl von Brexit-bezogenen Keywords deutet auf potentielle Reklassifikation hin

**Nächste Schritte:**
- LLM-Verarbeitung für präzisere Brexit-Klassifikation
- Analyse der Confidence Scores für Qualitätskontrolle
- Nutzung der umfangreichen Metadaten für erweiterte Analysen






