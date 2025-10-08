#!/usr/bin/env python3
"""
Test-Script für Brexit-Klassifizierung
Testet nur Januar 2016 Debatten
"""

import duckdb
import re
import os
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

# Lade .env Datei
load_dotenv()

# Konfiguration
DB_FILE = "../data/processed/debates.duckdb"
OUTPUT_DB = "../data/processed/debates_brexit_test_jan2016.duckdb"
GEMINI_MODEL = "gemini-2.0-flash-exp"


# Brexit-Keywords mit Gewichtung
DIRECT_KEYWORDS = [
    "brexit", "leave campaign", "remain campaign", "article 50",
    "referendum", "eu referendum", "european referendum",
    "leave the eu", "leaving the eu", "exit from europe",
    "withdrawal agreement", "divorce bill", "transition period",
    "hard brexit", "soft brexit", "no-deal brexit"
]

INDIRECT_KEYWORDS = [
    "european union", "european community", "eu membership",
    "brussels", "strasbourg", "european commission",
    "european parliament", "eurozone", "single market",
    "customs union", "free movement", "schengen",
    "eu law", "eu regulation", "eu directive",
    "eu budget", "eu contribution", "sovereignty",
    "independence", "british sovereignty", "take back control",
    "immigration control", "border control",
    "trade agreement", "trade deal", "wto",
    "northern ireland protocol", "backstop", "irish border"
]


def analyze_keywords(text):
    """
    Schritt 1: Keyword-basierte Analyse
    Gibt Confidence Score (0-1) und gefundene Keywords zurück
    """
    if not text:
        return 0.0, []

    text_lower = text.lower()

    found_direct = []
    found_indirect = []

    for keyword in DIRECT_KEYWORDS:
        if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
            found_direct.append(keyword)

    for keyword in INDIRECT_KEYWORDS:
        if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
            found_indirect.append(keyword)

    # Berechne Confidence Score
    direct_score = min(len(found_direct) * 0.3, 0.7)
    indirect_score = min(len(found_indirect) * 0.05, 0.3)

    confidence = min(direct_score + indirect_score, 1.0)
    all_found = found_direct + found_indirect

    return confidence, all_found


def analyze_with_gemini(debate_name, date, speeches_text, keywords_found, api_key, retry_count=0):
    """
    Schritt 2: LLM-basierte Analyse mit Gemini
    Gibt (has_brexit_relation: bool, confidence: float, reasoning: str, input_tokens: int, output_tokens: int) zurück
    """
    import time

    if not api_key:
        raise ValueError("GEMINI_API_KEY nicht gesetzt")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(GEMINI_MODEL)

    prompt = f"""You are analyzing UK parliamentary debates to determine if they relate to Brexit.

**Debate Information:**
- Topic: {debate_name}
- Date: {date}
- Keywords found: {', '.join(keywords_found[:10]) if keywords_found else 'None'}

**Speech excerpts (first 5 speeches):**
{speeches_text[:8000]}

**Task:**
Analyze whether this debate has a significant relation to Brexit (the UK's withdrawal from the European Union).

Consider:
- Direct mentions of Brexit, referendum, Article 50, withdrawal
- Discussions about EU membership, sovereignty, immigration from EU context
- Trade agreements in context of leaving EU
- Northern Ireland border issues related to Brexit

**Response format (JSON):**
{{
  "has_brexit_relation": true/false,
  "confidence": 0.0-1.0,
  "reasoning": "One sentence explanation"
}}

Respond ONLY with the JSON object, no additional text."""

    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()

        # Token-Zählung aus Response-Metadaten
        input_tokens = response.usage_metadata.prompt_token_count if hasattr(response, 'usage_metadata') else 0
        output_tokens = response.usage_metadata.candidates_token_count if hasattr(response, 'usage_metadata') else 0

        import json
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return (
                result.get('has_brexit_relation', False),
                float(result.get('confidence', 0.0)),
                result.get('reasoning', ''),
                input_tokens,
                output_tokens
            )
        else:
            print(f"  ⚠ Konnte JSON nicht parsen: {response_text[:100]}")
            return False, 0.0, "Failed to parse response", input_tokens, output_tokens

    except Exception as e:
        error_str = str(e)

        # Rate Limit Error (429) - Retry mit exponential backoff
        if "429" in error_str or "quota" in error_str.lower():
            if retry_count < 5:  # Max 5 Retries
                # Exponential backoff: 6, 12, 24, 48, 96 Sekunden
                wait_time = 6 * (2 ** retry_count)
                print(f"  ⚠ Rate Limit erreicht, warte {wait_time}s und versuche erneut... (Versuch {retry_count + 1}/5)")
                time.sleep(wait_time)
                return analyze_with_gemini(debate_name, date, speeches_text, keywords_found, api_key, retry_count + 1)
            else:
                print(f"  ✗ Rate Limit nach 5 Versuchen nicht behoben")
                return False, 0.0, f"Rate Limit Error after retries", 0, 0

        print(f"  ✗ Gemini API Fehler: {e}")
        return False, 0.0, f"API Error: {str(e)}", 0, 0


def combine_results(keyword_confidence, keyword_count, llm_has_relation, llm_confidence):
    """
    Schritt 3: Kombiniere Keyword- und LLM-Ergebnisse
    Gewichtung: 30% Keywords, 70% LLM
    """
    if keyword_count == 0:
        return False, 0.0

    combined_confidence = (0.3 * keyword_confidence) + (0.7 * llm_confidence)
    has_brexit_relation = combined_confidence > 0.5

    return has_brexit_relation, combined_confidence


def setup_output_database(source_db_path):
    """Erstellt eine Kopie der Datenbank mit zusätzlichen Brexit-Spalten"""
    print(f"Erstelle Test-Datenbank: {OUTPUT_DB}")

    conn_out = duckdb.connect(OUTPUT_DB)

    # Attach source database
    print("  Verbinde mit Quelldatenbank...")
    conn_out.execute(f"ATTACH '{source_db_path}' AS source_db (READ_ONLY)")

    # Kopiere nur Januar 2016 Daten
    print("  Kopiere Januar 2016 debates...")
    conn_out.execute("""
        CREATE TABLE debates AS
        SELECT * FROM source_db.debates
        WHERE date >= '2016-01-01' AND date < '2016-02-01'
    """)

    print("  Kopiere Januar 2016 topics...")
    conn_out.execute("""
        CREATE TABLE topics AS
        SELECT t.* FROM source_db.topics t
        JOIN debates d ON t.debate_id = d.debate_id
    """)

    print("  Kopiere Januar 2016 speeches...")
    conn_out.execute("""
        CREATE TABLE speeches AS
        SELECT s.* FROM source_db.speeches s
        JOIN debates d ON s.debate_id = d.debate_id
    """)

    # Füge Brexit-Spalten hinzu
    print("  Füge Brexit-Klassifizierungsspalten hinzu...")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_related BOOLEAN DEFAULT FALSE")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_confidence FLOAT DEFAULT 0.0")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_keyword_confidence FLOAT DEFAULT 0.0")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_llm_confidence FLOAT DEFAULT 0.0")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_keywords_found VARCHAR")
    conn_out.execute("ALTER TABLE speeches ADD COLUMN brexit_llm_reasoning VARCHAR")

    conn_out.commit()
    return conn_out


def main():
    print("=" * 70)
    print("BREXIT-KLASSIFIZIERUNG TEST - JANUAR 2016")
    print("=" * 70)

    # Prüfe API Key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("\n✗ FEHLER: GEMINI_API_KEY Umgebungsvariable nicht gesetzt!")
        print("Setze den API Key mit: export GEMINI_API_KEY='your-key-here'")
        return

    print(f"\n✓ Gemini API Key gefunden")
    print(f"✓ Verwende Modell: {GEMINI_MODEL}\n")

    # Verbinde mit Quelldatenbank
    print(f"Öffne Quelldatenbank: {DB_FILE}")
    if not Path(DB_FILE).exists():
        print(f"✗ Datenbank {DB_FILE} nicht gefunden!")
        return

    conn_source = duckdb.connect(DB_FILE, read_only=True)

    # Erstelle Output-Datenbank
    conn_out = setup_output_database(DB_FILE)

    # Hole alle Debatten aus Januar 2016
    debates = conn_source.execute("""
        SELECT DISTINCT
            d.debate_id,
            d.date,
            d.major_heading_text
        FROM debates d
        WHERE d.date >= '2016-01-01' AND d.date < '2016-02-01'
          AND d.major_heading_text IS NOT NULL
        ORDER BY d.date, d.debate_id
    """).fetchall()

    print(f"\nGefunden: {len(debates)} Debatten in Januar 2016\n")
    print("Starte Klassifizierung...\n")

    # Statistiken
    total_processed = 0
    total_with_keywords = 0
    total_llm_analyzed = 0
    total_brexit_related = 0

    # Cost Tracking (Gemini 2.0 Flash Pricing)
    # Input: $0.075 per 1M tokens
    # Output: $0.30 per 1M tokens
    total_input_tokens = 0
    total_output_tokens = 0
    total_api_calls = 0

    # Zeit-Tracking
    import time
    start_time = time.time()

    # Rate Limiting: 10 Requests/Minute
    # Warte 6 Sekunden zwischen API Calls (=10 Requests/Minute)
    REQUEST_DELAY = 6.0  # Sekunden

    # Detaillierte Ergebnisse sammeln
    results = []
    last_api_call_time = 0

    # Verarbeite jede Debatte
    for i, (debate_id, date, debate_name) in enumerate(debates, 1):
        print(f"[{i}/{len(debates)}] {date} - {debate_name[:60]}")

        # Hole ersten 5 Redebeiträge
        speeches = conn_source.execute("""
            SELECT speech_id, speech_text
            FROM speeches
            WHERE debate_id = ?
            AND speech_text IS NOT NULL
            ORDER BY speech_id
            LIMIT 5
        """, [debate_id]).fetchall()

        if not speeches:
            print("  → Keine Reden gefunden, überspringe\n")
            continue

        # Kombiniere Texte
        combined_text = "\n\n".join([s[1] for s in speeches])

        # SCHRITT 1: Keyword-Analyse
        keyword_conf, keywords_found = analyze_keywords(combined_text)

        print(f"  Keywords: {len(keywords_found)} gefunden, Confidence: {keyword_conf:.2f}")
        if keywords_found:
            print(f"    → {', '.join(keywords_found[:5])}")

        # Wenn keine Keywords gefunden, überspringe
        if len(keywords_found) == 0:
            total_processed += 1
            results.append({
                'date': date,
                'debate': debate_name[:50],
                'keywords': 0,
                'keyword_conf': 0.0,
                'llm_conf': 0.0,
                'final': False,
                'final_conf': 0.0,
                'reasoning': 'No keywords found'
            })
            print()
            continue

        total_with_keywords += 1

        # Rate Limiting: Warte 6 Sekunden zwischen API Calls
        if last_api_call_time > 0:
            time_since_last_call = time.time() - last_api_call_time
            if time_since_last_call < REQUEST_DELAY:
                wait_time = REQUEST_DELAY - time_since_last_call
                print(f"  ⏱️  Rate Limit: Warte {wait_time:.1f}s...")
                time.sleep(wait_time)

        # SCHRITT 2: LLM-Analyse
        print(f"  Analysiere mit Gemini...")
        last_api_call_time = time.time()
        llm_has_relation, llm_conf, llm_reasoning, input_tokens, output_tokens = analyze_with_gemini(
            debate_name,
            date,
            combined_text,
            keywords_found,
            api_key
        )

        total_llm_analyzed += 1
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        total_api_calls += 1

        print(f"  LLM: {llm_has_relation}, Confidence: {llm_conf:.2f}")
        print(f"  Tokens: {input_tokens} in, {output_tokens} out")
        print(f"  Reasoning: {llm_reasoning}")

        # SCHRITT 3: Kombiniere Ergebnisse
        brexit_related, final_conf = combine_results(
            keyword_conf,
            len(keywords_found),
            llm_has_relation,
            llm_conf
        )

        print(f"  ✓ Final: Brexit-Bezug = {brexit_related}, Confidence = {final_conf:.2f}")

        if brexit_related:
            total_brexit_related += 1

        # Speichere Ergebnis
        results.append({
            'date': date,
            'debate': debate_name[:50],
            'keywords': len(keywords_found),
            'keyword_conf': keyword_conf,
            'llm_conf': llm_conf,
            'final': brexit_related,
            'final_conf': final_conf,
            'reasoning': llm_reasoning
        })

        # Update alle Reden dieser Debatte
        conn_out.execute("""
            UPDATE speeches
            SET
                brexit_related = ?,
                brexit_confidence = ?,
                brexit_keyword_confidence = ?,
                brexit_llm_confidence = ?,
                brexit_keywords_found = ?,
                brexit_llm_reasoning = ?
            WHERE debate_id = ?
        """, [
            brexit_related,
            final_conf,
            keyword_conf,
            llm_conf,
            ', '.join(keywords_found[:10]),
            llm_reasoning,
            debate_id
        ])

        conn_out.commit()
        total_processed += 1

        print()

    # Zeit-Messung
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Zusammenfassung
    print("=" * 70)
    print("TEST ABGESCHLOSSEN!")
    print("=" * 70)
    print(f"\nStatistiken:")
    print(f"  Debatten analysiert:        {total_processed}")
    print(f"  Mit Keywords:               {total_with_keywords}")
    print(f"  LLM-analysiert:             {total_llm_analyzed}")
    print(f"  Brexit-bezogen:             {total_brexit_related}")
    if total_processed > 0:
        print(f"  Brexit-Rate:                {total_brexit_related/total_processed*100:.1f}%")
    print(f"\n⏱️  Verarbeitungszeit:")
    print(f"  Gesamt:                     {elapsed_time:.1f} Sekunden ({elapsed_time/60:.1f} Minuten)")
    if total_llm_analyzed > 0:
        print(f"  Pro API Call:               {elapsed_time/total_llm_analyzed:.2f} Sekunden")

    # Kosten-Berechnung
    print(f"\n" + "=" * 70)
    print("KOSTEN-ANALYSE (GEMINI 2.0 FLASH)")
    print("=" * 70)

    # Gemini 2.0 Flash Pricing (Stand Dez 2024)
    INPUT_PRICE_PER_1M = 0.075  # $0.075 per 1M input tokens
    OUTPUT_PRICE_PER_1M = 0.30  # $0.30 per 1M output tokens

    input_cost = (total_input_tokens / 1_000_000) * INPUT_PRICE_PER_1M
    output_cost = (total_output_tokens / 1_000_000) * OUTPUT_PRICE_PER_1M
    total_cost = input_cost + output_cost

    print(f"\nJanuar 2016 (Test):")
    print(f"  API Calls:              {total_api_calls}")
    print(f"  Input Tokens:           {total_input_tokens:,}")
    print(f"  Output Tokens:          {total_output_tokens:,}")
    print(f"  Kosten Input:           ${input_cost:.4f}")
    print(f"  Kosten Output:          ${output_cost:.4f}")
    print(f"  Gesamtkosten:           ${total_cost:.4f}")

    # Hochrechnung für alle Debatten
    print(f"\n" + "-" * 70)
    print("HOCHRECHNUNG FÜR ALLE DEBATTEN (2012-2022)")
    print("-" * 70)

    # Hole Gesamtzahl der Debatten
    total_all_debates = conn_source.execute("SELECT COUNT(DISTINCT debate_id) FROM debates WHERE major_heading_text IS NOT NULL").fetchone()[0]

    # Annahme: Gleiche Rate von Debatten mit Keywords
    if total_with_keywords > 0:
        keyword_rate = total_with_keywords / total_processed
        estimated_debates_with_keywords = int(total_all_debates * keyword_rate)

        # Durchschnittliche Tokens pro API Call
        avg_input_tokens = total_input_tokens / total_api_calls if total_api_calls > 0 else 0
        avg_output_tokens = total_output_tokens / total_api_calls if total_api_calls > 0 else 0

        estimated_total_input = estimated_debates_with_keywords * avg_input_tokens
        estimated_total_output = estimated_debates_with_keywords * avg_output_tokens

        estimated_input_cost = (estimated_total_input / 1_000_000) * INPUT_PRICE_PER_1M
        estimated_output_cost = (estimated_total_output / 1_000_000) * OUTPUT_PRICE_PER_1M
        estimated_total_cost = estimated_input_cost + estimated_output_cost

        print(f"  Debatten gesamt:        {total_all_debates:,}")
        print(f"  Est. mit Keywords:      {estimated_debates_with_keywords:,} ({keyword_rate*100:.1f}%)")
        print(f"  Avg. Tokens/Call:       {avg_input_tokens:.0f} in, {avg_output_tokens:.0f} out")
        print(f"  Est. API Calls:         {estimated_debates_with_keywords:,}")
        print(f"  Est. Input Tokens:      {estimated_total_input:,.0f}")
        print(f"  Est. Output Tokens:     {estimated_total_output:,.0f}")
        print(f"  Est. Gesamtkosten:      ${estimated_total_cost:.2f}")

        # Zeit-Hochrechnung
        if total_llm_analyzed > 0:
            avg_time_per_call = elapsed_time / total_llm_analyzed
            estimated_total_time = estimated_debates_with_keywords * avg_time_per_call
            estimated_hours = estimated_total_time / 3600
            estimated_days = estimated_hours / 24

            print(f"\n⏱️  ZEIT-HOCHRECHNUNG:")
            print(f"  Avg. Zeit/API Call:     {avg_time_per_call:.2f} Sekunden")
            print(f"  Est. Gesamtzeit:        {estimated_total_time:,.0f} Sekunden")
            print(f"                          {estimated_total_time/60:,.1f} Minuten")
            print(f"                          {estimated_hours:.1f} Stunden")
            if estimated_days >= 1:
                print(f"                          {estimated_days:.1f} Tage")

            print(f"\n💡 Geschätzte Kosten: ${estimated_total_cost:.2f}")
            print(f"💡 Geschätzte Dauer: {estimated_hours:.1f} Stunden ({estimated_days:.1f} Tage)")
        else:
            print(f"\n💡 Geschätzte Kosten für vollständige Klassifizierung: ${estimated_total_cost:.2f}")
    else:
        print("  Keine Daten für Hochrechnung verfügbar.")

    # Detaillierte Ergebnistabelle
    print("\n" + "=" * 70)
    print("DETAILLIERTE ERGEBNISSE")
    print("=" * 70)
    print(f"{'Datum':<12} {'Keywords':<8} {'KW-Conf':<8} {'LLM-Conf':<9} {'Final':<7} {'Conf':<6} {'Debatte':<30}")
    print("-" * 70)
    for r in results:
        final_str = "✓ YES" if r['final'] else "✗ NO"
        print(f"{r['date']:<12} {r['keywords']:<8} {r['keyword_conf']:<8.2f} {r['llm_conf']:<9.2f} {final_str:<7} {r['final_conf']:<6.2f} {r['debate']:<30}")

    # Brexit-bezogene Debatten hervorheben
    brexit_debates = [r for r in results if r['final']]
    if brexit_debates:
        print("\n" + "=" * 70)
        print(f"BREXIT-BEZOGENE DEBATTEN ({len(brexit_debates)})")
        print("=" * 70)
        for r in brexit_debates:
            print(f"\n{r['date']} - {r['debate']}")
            print(f"  Confidence: {r['final_conf']:.2f} (Keywords: {r['keyword_conf']:.2f}, LLM: {r['llm_conf']:.2f})")
            print(f"  Reasoning: {r['reasoning']}")

    # Datenbank-Statistiken
    brexit_speeches = conn_out.execute("""
        SELECT COUNT(*) FROM speeches WHERE brexit_related = TRUE
    """).fetchone()[0]

    total_speeches = conn_out.execute("""
        SELECT COUNT(*) FROM speeches
    """).fetchone()[0]

    print(f"\n" + "=" * 70)
    print(f"Reden mit Brexit-Bezug: {brexit_speeches:,} von {total_speeches:,}")

    conn_source.close()
    conn_out.close()

    print(f"\n✓ Test-Ergebnisse gespeichert in: {OUTPUT_DB}")
    print("\nÖffne die Datenbank mit:")
    print(f"  duckdb {OUTPUT_DB}")
    print("  SELECT * FROM speeches WHERE brexit_related = TRUE;")


if __name__ == "__main__":
    main()
