#!/usr/bin/env python3
"""
Alternative Migration: Export von DuckDB zu CSV, dann Import zu Railway
"""

import duckdb
import pandas as pd
import argparse
from pathlib import Path
import json

def export_duckdb_to_csv(duckdb_path: str, output_dir: str):
    """Exportiert DuckDB Daten zu CSV-Dateien"""
    
    print("🔄 Exportiere DuckDB zu CSV-Dateien...")
    
    # Erstelle Output-Verzeichnis
    Path(output_dir).mkdir(exist_ok=True)
    
    # DuckDB Verbindung
    duckdb_conn = duckdb.connect(duckdb_path, read_only=True)
    
    try:
        # Exportiere Chunks
        print("📥 Exportiere Chunks...")
        chunks_data = duckdb_conn.execute("SELECT * FROM chunks").fetchall()
        chunks_columns = [desc[0] for desc in duckdb_conn.execute("DESCRIBE chunks").fetchall()]
        
        # Erstelle DataFrame manuell
        chunks_df = pd.DataFrame(chunks_data, columns=chunks_columns)
        chunks_csv = Path(output_dir) / "chunks.csv"
        chunks_df.to_csv(chunks_csv, index=False)
        print(f"✅ Chunks exportiert: {chunks_csv}")
        
        # Exportiere Agreement (falls vorhanden)
        try:
            agreement_data = duckdb_conn.execute("SELECT * FROM agreement_chunks").fetchall()
            if agreement_data:
                agreement_columns = [desc[0] for desc in duckdb_conn.execute("DESCRIBE agreement_chunks").fetchall()]
                agreement_df = pd.DataFrame(agreement_data, columns=agreement_columns)
                agreement_csv = Path(output_dir) / "agreement_chunks.csv"
                agreement_df.to_csv(agreement_csv, index=False)
                print(f"✅ Agreement exportiert: {agreement_csv}")
            else:
                print("⚠️ Keine Agreement-Daten gefunden")
        except Exception as e:
            print(f"⚠️ Agreement-Tabelle nicht gefunden: {e}")
        
        # Statistiken
        total_chunks = len(chunks_df)
        annotated_chunks = len(chunks_df[chunks_df['frame_label'].notna()])
        assigned_chunks = len(chunks_df[chunks_df['assigned_user'].notna() & (chunks_df['assigned_user'] != '')])
        
        print(f"\n📊 Export-Statistiken:")
        print(f"  📝 Gesamt Chunks: {total_chunks:,}")
        print(f"  🏷️ Annotierte Chunks: {annotated_chunks:,}")
        print(f"  👥 Zugewiesene Chunks: {assigned_chunks:,}")
        
        # Erstelle SQL-Import-Script
        create_sql_script(output_dir, chunks_df)
        
    except Exception as e:
        print(f"❌ Fehler beim Export: {e}")
        raise
    finally:
        duckdb_conn.close()

def create_sql_script(output_dir: str, chunks_df: pd.DataFrame):
    """Erstellt SQL-Script für Railway Import"""
    
    sql_script = Path(output_dir) / "import_to_railway.sql"
    
    with open(sql_script, 'w') as f:
        f.write("-- SQL Script für Railway PostgreSQL Import\n")
        f.write("-- Führe dieses Script in deiner Railway PostgreSQL aus\n\n")
        
        # Erstelle Tabellen
        f.write("-- Erstelle Tabellen\n")
        f.write("""
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id VARCHAR(255) PRIMARY KEY,
    speech_id VARCHAR(255),
    debate_id VARCHAR(255),
    speaker_name VARCHAR(255),
    speaker_party VARCHAR(255),
    debate_title TEXT,
    debate_date DATE,
    chunk_text TEXT,
    chunk_index INTEGER,
    total_chunks INTEGER,
    word_count INTEGER,
    char_count INTEGER,
    chunking_method VARCHAR(100),
    assigned_user VARCHAR(255),
    frame_label VARCHAR(100),
    annotation_confidence INTEGER,
    annotation_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agreement_chunks (
    chunk_id VARCHAR(255) PRIMARY KEY,
    annotator1 VARCHAR(255),
    annotator2 VARCHAR(255),
    label1 VARCHAR(100),
    label2 VARCHAR(100),
    agreement_score DECIMAL(3,2),
    agreement_perfect BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Erstelle Indizes
CREATE INDEX IF NOT EXISTS idx_chunks_assigned_user ON chunks(assigned_user);
CREATE INDEX IF NOT EXISTS idx_chunks_frame_label ON chunks(frame_label);
CREATE INDEX IF NOT EXISTS idx_chunks_speaker ON chunks(speaker_name);
CREATE INDEX IF NOT EXISTS idx_chunks_debate ON chunks(debate_id);
""")
        
        f.write("\n-- CSV Import (führe nach dem Upload der CSV-Dateien aus)\n")
        f.write("-- COPY chunks FROM '/path/to/chunks.csv' WITH CSV HEADER;\n")
        f.write("-- COPY agreement_chunks FROM '/path/to/agreement_chunks.csv' WITH CSV HEADER;\n")
    
    print(f"✅ SQL-Script erstellt: {sql_script}")

def main():
    parser = argparse.ArgumentParser(description="Exportiere DuckDB zu CSV für Railway Import")
    parser.add_argument("--duckdb", required=True, help="Pfad zur DuckDB-Datei")
    parser.add_argument("--output-dir", default="railway_export", help="Output-Verzeichnis für CSV-Dateien")
    
    args = parser.parse_args()
    
    # Prüfe DuckDB-Datei
    if not Path(args.duckdb).exists():
        print(f"❌ DuckDB-Datei nicht gefunden: {args.duckdb}")
        return
    
    # Führe Export durch
    export_duckdb_to_csv(args.duckdb, args.output_dir)
    
    print(f"\n🎯 Nächste Schritte:")
    print(f"1. Lade die CSV-Dateien zu Railway hoch")
    print(f"2. Führe das SQL-Script in Railway PostgreSQL aus")
    print(f"3. Oder nutze Railway Dashboard für den Import")

if __name__ == "__main__":
    main()
