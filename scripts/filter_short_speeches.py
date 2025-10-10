#!/usr/bin/env python3
"""
Filtert Reden mit weniger als 20 Wörtern aus der Datenbank
Erstellt eine neue Datenbank mit nur den Reden, die mindestens 20 Wörter haben
"""

import duckdb
from pathlib import Path

# Konfiguration
# Pfade zu den Datenbanken im data/processed Verzeichnis
INPUT_DB = "data/processed/debates_brexit_filtered.duckdb"
OUTPUT_DB = "data/processed/debates_brexit_filtered_min20words.duckdb"

def count_words(text):
    """Zählt die Wörter in einem Text"""
    if not text:
        return 0
    return len(text.split())

def get_table_schema(conn, table_name):
    """Holt das Schema einer Tabelle"""
    schema_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = []
    for row in schema_info:
        col_name = row[1]
        col_type = row[2]
        is_nullable = row[3] == 0
        default_value = row[4]
        is_pk = row[5] == 1
        
        col_def = f"{col_name} {col_type}"
        if not is_nullable:
            col_def += " NOT NULL"
        if default_value is not None:
            col_def += f" DEFAULT {default_value}"
        if is_pk:
            col_def += " PRIMARY KEY"
        
        columns.append(col_def)
    
    return columns

def create_table_from_schema(conn, table_name, columns):
    """Erstellt eine Tabelle basierend auf dem Schema"""
    create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n)"
    conn.execute(create_sql)

def main():
    print("=" * 70)
    print("FILTERUNG VON KURZEN REDEN (< 20 WÖRTER)")
    print("=" * 70)
    
    # Prüfe Input-Datenbank
    if not Path(INPUT_DB).exists():
        print(f"\n✗ Input-Datenbank {INPUT_DB} nicht gefunden!")
        return
    
    print(f"\n✓ Input-Datenbank gefunden: {INPUT_DB}")
    
    # Öffne Input-Datenbank
    conn_source = duckdb.connect(INPUT_DB, read_only=True)
    
    # Erstelle Output-Datenbank
    print(f"Erstelle Output-Datenbank: {OUTPUT_DB}")
    conn_target = duckdb.connect(OUTPUT_DB)
    
    # Hole alle Tabellennamen
    tables = [row[0] for row in conn_source.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    print(f"Gefundene Tabellen: {', '.join(tables)}")
    
    # Kopiere Schema für alle Tabellen
    print("\nErstelle Tabellenschema...")
    for table_name in tables:
        print(f"  Erstelle Schema für {table_name}...")
        columns = get_table_schema(conn_source, table_name)
        create_table_from_schema(conn_target, table_name, columns)
    
    # Kopiere Daten
    print("\nKopiere und filtere Daten...")
    
    # Statistiken für Speeches
    total_speeches_input = conn_source.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
    print(f"\n  Analysiere {total_speeches_input:,} Reden...")
    
    # Filtere Speeches basierend auf Wortanzahl (mindestens 20 Wörter)
    print("  Kopiere speeches (nur mit >= 20 Wörtern)...")
    
    # Hole alle Speeches
    speeches = conn_source.execute("SELECT * FROM speeches").fetchall()
    
    # Hole Spaltennamen
    speech_columns = [row[1] for row in conn_source.execute("PRAGMA table_info('speeches')").fetchall()]
    
    # Finde Index der speech_text Spalte
    speech_text_idx = speech_columns.index('speech_text')
    speech_id_idx = speech_columns.index('speech_id')
    debate_id_idx = speech_columns.index('debate_id')
    topic_id_idx = speech_columns.index('topic_id') if 'topic_id' in speech_columns else None
    
    # Filtere und füge Speeches ein
    filtered_speeches = []
    filtered_speech_ids = set()
    filtered_debate_ids = set()
    filtered_topic_ids = set()
    
    for row in speeches:
        speech_text = row[speech_text_idx]
        word_count = count_words(speech_text)
        
        if word_count >= 20:
            filtered_speeches.append(row)
            filtered_speech_ids.add(row[speech_id_idx])
            if row[debate_id_idx]:
                filtered_debate_ids.add(row[debate_id_idx])
            if topic_id_idx is not None and row[topic_id_idx]:
                filtered_topic_ids.add(row[topic_id_idx])
    
    # Füge gefilterte Speeches ein
    placeholders = ", ".join(["?" for _ in speech_columns])
    insert_sql = f"INSERT INTO speeches VALUES ({placeholders})"
    
    for row in filtered_speeches:
        conn_target.execute(insert_sql, row)
    
    print(f"    ✓ {len(filtered_speeches):,} Reden kopiert (>= 20 Wörter)")
    print(f"    ✗ {total_speeches_input - len(filtered_speeches):,} Reden gefiltert (< 20 Wörter)")
    
    # Kopiere nur die zugehörigen Debates
    print("\n  Kopiere zugehörige debates...")
    if "debates" in tables:
        debates_data = conn_source.execute("SELECT * FROM debates").fetchall()
        debate_columns = [row[1] for row in conn_source.execute("PRAGMA table_info('debates')").fetchall()]
        debate_id_idx_debates = debate_columns.index('debate_id')
        
        placeholders = ", ".join(["?" for _ in debate_columns])
        insert_sql = f"INSERT INTO debates VALUES ({placeholders})"
        
        debates_copied = 0
        for row in debates_data:
            if row[debate_id_idx_debates] in filtered_debate_ids:
                conn_target.execute(insert_sql, row)
                debates_copied += 1
        
        print(f"    ✓ {debates_copied:,} debates kopiert")
    
    # Kopiere nur die zugehörigen Topics
    print("  Kopiere zugehörige topics...")
    if "topics" in tables:
        topics_data = conn_source.execute("SELECT * FROM topics").fetchall()
        topic_columns = [row[1] for row in conn_source.execute("PRAGMA table_info('topics')").fetchall()]
        
        # Filtere Topics nach debate_id und/oder topic_id
        topic_id_idx_topics = topic_columns.index('topic_id') if 'topic_id' in topic_columns else None
        debate_id_idx_topics = topic_columns.index('debate_id') if 'debate_id' in topic_columns else None
        
        placeholders = ", ".join(["?" for _ in topic_columns])
        insert_sql = f"INSERT INTO topics VALUES ({placeholders})"
        
        topics_copied = 0
        for row in topics_data:
            should_copy = False
            
            # Prüfe ob Topic zu gefilterten Speeches gehört
            if topic_id_idx_topics is not None and row[topic_id_idx_topics] in filtered_topic_ids:
                should_copy = True
            elif debate_id_idx_topics is not None and row[debate_id_idx_topics] in filtered_debate_ids:
                should_copy = True
            
            if should_copy:
                conn_target.execute(insert_sql, row)
                topics_copied += 1
        
        print(f"    ✓ {topics_copied:,} topics kopiert")
    
    # Statistiken
    print("\n" + "=" * 70)
    print("STATISTIKEN")
    print("=" * 70)
    
    # Input-Statistiken
    total_speeches_input = conn_source.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
    total_debates_input = conn_source.execute("SELECT COUNT(*) FROM debates").fetchone()[0] if "debates" in tables else 0
    total_topics_input = conn_source.execute("SELECT COUNT(*) FROM topics").fetchone()[0] if "topics" in tables else 0
    
    # Output-Statistiken
    total_speeches_output = conn_target.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
    total_debates_output = conn_target.execute("SELECT COUNT(*) FROM debates").fetchone()[0] if "debates" in tables else 0
    total_topics_output = conn_target.execute("SELECT COUNT(*) FROM topics").fetchone()[0] if "topics" in tables else 0
    
    print(f"\nInput-Datenbank ({INPUT_DB}):")
    print(f"  Speeches:  {total_speeches_input:,}")
    print(f"  Debatten:  {total_debates_input:,}")
    print(f"  Topics:    {total_topics_input:,}")
    
    print(f"\nOutput-Datenbank ({OUTPUT_DB}):")
    print(f"  Speeches:  {total_speeches_output:,}")
    print(f"  Debatten:  {total_debates_output:,}")
    print(f"  Topics:    {total_topics_output:,}")
    
    filtered_count = total_speeches_input - total_speeches_output
    retention_rate = (total_speeches_output / max(total_speeches_input, 1)) * 100
    
    print(f"\nFilterung:")
    print(f"  Gefiltert:         {filtered_count:,} Reden (< 20 Wörter)")
    print(f"  Behalten:          {total_speeches_output:,} Reden (>= 20 Wörter)")
    print(f"  Behaltungsrate:    {retention_rate:.1f}%")
    
    # Schließe Verbindungen
    conn_source.close()
    conn_target.close()
    
    print(f"\n✓ Gefilterte Datenbank erstellt: {OUTPUT_DB}")
    print(f"✓ Nur Reden mit mindestens 20 Wörtern wurden übertragen")

if __name__ == "__main__":
    main()

