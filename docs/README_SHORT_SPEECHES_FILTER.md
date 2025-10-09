# Filterung von kurzen Reden

## Übersicht

Das Script `filter_short_speeches.py` filtert alle Reden aus der Datenbank `debates_brexit_filtered.duckdb`, die weniger als 20 Wörter enthalten.

## Funktionsweise

1. **Input**: `debates_brexit_filtered.duckdb`
2. **Output**: `debates_brexit_filtered_min20words.duckdb`
3. **Filter**: Reden mit weniger als 20 Wörtern werden entfernt
4. **Datenerhaltung**: Alle anderen Daten (debates, topics) bleiben erhalten und werden entsprechend gefiltert

## Verwendung

### Option 1: Batch-Datei (Empfohlen für Windows)

Doppelklicke auf die Datei `run_filter_short_speeches.bat` im Hauptverzeichnis.

### Option 2: Direkter Python-Aufruf

Öffne ein Terminal im Hauptverzeichnis und führe aus:

```bash
python scripts/filter_short_speeches.py
```

oder

```bash
py scripts/filter_short_speeches.py
```

## Output

Das Script erstellt eine neue Datenbank `debates_brexit_filtered_min20words.duckdb` mit:

- Alle Tabellen aus der Ursprungsdatenbank (speeches, debates, topics)
- Nur Reden mit >= 20 Wörtern
- Zugehörige Debatten und Topics werden automatisch mitgefiltert

## Statistiken

Am Ende der Ausführung zeigt das Script:

- Anzahl gefilterter Reden (< 20 Wörter)
- Anzahl behaltener Reden (>= 20 Wörter)
- Behaltungsrate in Prozent
- Anzahl der kopierten Debatten und Topics

## Wichtige Hinweise

- Die Ursprungsdatenbank wird **nicht verändert** (read-only Modus)
- Die neue Datenbank wird im gleichen Ordner wie die Ursprungsdatenbank erstellt
- Bei bereits existierender Output-Datenbank wird diese überschrieben
- Die Wortanzahl wird durch einfaches Splitten am Leerzeichen berechnet

## Beispiel-Output

```
======================================================================
FILTERUNG VON KURZEN REDEN (< 20 WÖRTER)
======================================================================

✓ Input-Datenbank gefunden: debates_brexit_filtered.duckdb
Erstelle Output-Datenbank: debates_brexit_filtered_min20words.duckdb
Gefundene Tabellen: debates, topics, speeches

Erstelle Tabellenschema...
  Erstelle Schema für debates...
  Erstelle Schema für topics...
  Erstelle Schema für speeches...

Kopiere und filtere Daten...

  Analysiere 125,432 Reden...
  Kopiere speeches (nur mit >= 20 Wörtern)...
    ✓ 120,156 Reden kopiert (>= 20 Wörter)
    ✗ 5,276 Reden gefiltert (< 20 Wörter)

  Kopiere zugehörige debates...
    ✓ 2,345 debates kopiert

  Kopiere zugehörige topics...
    ✓ 8,123 topics kopiert

======================================================================
STATISTIKEN
======================================================================

Input-Datenbank (debates_brexit_filtered.duckdb):
  Speeches:  125,432
  Debatten:  2,345
  Topics:    8,123

Output-Datenbank (debates_brexit_filtered_min20words.duckdb):
  Speeches:  120,156
  Debatten:  2,345
  Topics:    8,123

Filterung:
  Gefiltert:         5,276 Reden (< 20 Wörter)
  Behalten:          120,156 Reden (>= 20 Wörter)
  Behaltungsrate:    95.8%

✓ Gefilterte Datenbank erstellt: debates_brexit_filtered_min20words.duckdb
✓ Nur Reden mit mindestens 20 Wörtern wurden übertragen
```

## Technische Details

### Wortanzahl-Berechnung

```python
def count_words(text):
    """Zählt die Wörter in einem Text"""
    if not text:
        return 0
    return len(text.split())
```

Die Wortanzahl wird durch Aufteilen des Textes an Leerzeichen berechnet. Dies ist eine einfache, aber effektive Methode für die meisten Fälle.

### Datenbank-Struktur

Die Output-Datenbank behält die vollständige Struktur der Input-Datenbank bei:

- **debates**: Informationen über Parlamentsdebatten
- **topics**: Themen innerhalb der Debatten
- **speeches**: Einzelne Reden (gefiltert nach Wortanzahl)

### Performance

- Das Script verarbeitet alle Reden im Speicher
- Für sehr große Datenbanken (> 1 Million Reden) könnte eine Batch-Verarbeitung notwendig sein
- Die aktuelle Implementierung ist für mittelgroße Datenbanken (< 500k Reden) optimiert

