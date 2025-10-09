#!/usr/bin/env python3
"""
Migration von DuckDB zu Railway PostgreSQL
Konvertiert die chunks-Tabelle von DuckDB zu Railway PostgreSQL
"""

import duckdb
import psycopg2
import argparse
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime
import os

def create_postgresql_schema(cursor):
    """Erstellt das PostgreSQL Schema"""
    
    # Chunks-Tabelle
    cursor.execute("""
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
    """)
    
    # Agreement-Tabelle
    cursor.execute("""
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
    """)
    
    # Indizes für bessere Performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_assigned_user ON chunks(assigned_user);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_frame_label ON chunks(frame_label);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_speaker ON chunks(speaker_name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_debate ON chunks(debate_id);")

def migrate_chunks_data(duckdb_path: str, railway_db_url: str):
    """Migriert Chunks-Daten von DuckDB zu Railway PostgreSQL"""
    
    print("🔄 Starte Migration von DuckDB zu Railway PostgreSQL...")
    
    # DuckDB Verbindung
    duckdb_conn = duckdb.connect(duckdb_path, read_only=True)
    
    # Railway PostgreSQL Verbindung
    pg_conn = psycopg2.connect(railway_db_url)
    pg_cursor = pg_conn.cursor()
    
    try:
        # Erstelle Schema
        print("📋 Erstelle PostgreSQL Schema...")
        create_postgresql_schema(pg_cursor)
        
        # Lade Chunks aus DuckDB
        print("📥 Lade Chunks aus DuckDB...")
        chunks = duckdb_conn.execute("SELECT * FROM chunks").fetchall()
        
        # Hole Spalten-Namen
        columns = [desc[0] for desc in duckdb_conn.execute("DESCRIBE chunks").fetchall()]
        print(f"📊 Gefunden: {len(chunks)} Chunks mit {len(columns)} Spalten")
        
        # Migriere Chunks
        print("🔄 Migriere Chunks-Daten...")
        insert_sql = """
            INSERT INTO chunks (
                chunk_id, speech_id, debate_id, speaker_name, speaker_party,
                debate_title, debate_date, chunk_text, chunk_index, total_chunks,
                word_count, char_count, chunking_method, assigned_user,
                frame_label, annotation_confidence, annotation_notes,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (chunk_id) DO UPDATE SET
                speech_id = EXCLUDED.speech_id,
                debate_id = EXCLUDED.debate_id,
                speaker_name = EXCLUDED.speaker_name,
                speaker_party = EXCLUDED.speaker_party,
                debate_title = EXCLUDED.debate_title,
                debate_date = EXCLUDED.debate_date,
                chunk_text = EXCLUDED.chunk_text,
                chunk_index = EXCLUDED.chunk_index,
                total_chunks = EXCLUDED.total_chunks,
                word_count = EXCLUDED.word_count,
                char_count = EXCLUDED.char_count,
                chunking_method = EXCLUDED.chunking_method,
                assigned_user = EXCLUDED.assigned_user,
                frame_label = EXCLUDED.frame_label,
                annotation_confidence = EXCLUDED.annotation_confidence,
                annotation_notes = EXCLUDED.annotation_notes,
                updated_at = CURRENT_TIMESTAMP
        """
        
        for i, chunk in enumerate(chunks):
            if i % 100 == 0:
                print(f"  📝 Verarbeitet: {i}/{len(chunks)}")
            
            # Konvertiere Daten für PostgreSQL
            chunk_data = list(chunk)
            
            # Konvertiere None zu None für PostgreSQL
            for j, value in enumerate(chunk_data):
                if value is None:
                    chunk_data[j] = None
                elif isinstance(value, str) and value.strip() == '':
                    chunk_data[j] = None
            
            pg_cursor.execute(insert_sql, chunk_data)
        
        # Prüfe ob Agreement-Tabelle existiert
        try:
            agreement_chunks = duckdb_conn.execute("SELECT * FROM agreement_chunks").fetchall()
            if agreement_chunks:
                print(f"🔄 Migriere {len(agreement_chunks)} Agreement-Chunks...")
                
                agreement_insert_sql = """
                    INSERT INTO agreement_chunks (
                        chunk_id, annotator1, annotator2, label1, label2,
                        agreement_score, agreement_perfect, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chunk_id) DO UPDATE SET
                        annotator1 = EXCLUDED.annotator1,
                        annotator2 = EXCLUDED.annotator2,
                        label1 = EXCLUDED.label1,
                        label2 = EXCLUDED.label2,
                        agreement_score = EXCLUDED.agreement_score,
                        agreement_perfect = EXCLUDED.agreement_perfect,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                for agreement in agreement_chunks:
                    agreement_data = list(agreement)
                    pg_cursor.execute(agreement_insert_sql, agreement_data)
                    
        except Exception as e:
            print(f"⚠️ Agreement-Tabelle nicht gefunden oder leer: {e}")
        
        # Commit Änderungen
        pg_conn.commit()
        print("✅ Migration erfolgreich abgeschlossen!")
        
        # Statistiken
        pg_cursor.execute("SELECT COUNT(*) FROM chunks")
        total_chunks = pg_cursor.fetchone()[0]
        
        pg_cursor.execute("SELECT COUNT(*) FROM chunks WHERE frame_label IS NOT NULL")
        annotated_chunks = pg_cursor.fetchone()[0]
        
        pg_cursor.execute("SELECT COUNT(*) FROM chunks WHERE assigned_user IS NOT NULL AND assigned_user != ''")
        assigned_chunks = pg_cursor.fetchone()[0]
        
        print(f"\n📊 Railway PostgreSQL Statistiken:")
        print(f"  📝 Gesamt Chunks: {total_chunks:,}")
        print(f"  🏷️ Annotierte Chunks: {annotated_chunks:,}")
        print(f"  👥 Zugewiesene Chunks: {assigned_chunks:,}")
        
    except Exception as e:
        print(f"❌ Fehler bei der Migration: {e}")
        pg_conn.rollback()
        raise
    finally:
        duckdb_conn.close()
        pg_cursor.close()
        pg_conn.close()

def main():
    parser = argparse.ArgumentParser(description="Migriere DuckDB zu Railway PostgreSQL")
    parser.add_argument("--duckdb", required=True, help="Pfad zur DuckDB-Datei")
    parser.add_argument("--railway-url", help="Railway DATABASE_URL (optional, kann auch als Umgebungsvariable gesetzt werden)")
    
    args = parser.parse_args()
    
    # Prüfe DuckDB-Datei
    if not Path(args.duckdb).exists():
        print(f"❌ DuckDB-Datei nicht gefunden: {args.duckdb}")
        return
    
    # Railway DATABASE_URL
    railway_url = args.railway_url or os.getenv('DATABASE_URL')
    if not railway_url:
        print("❌ Railway DATABASE_URL nicht gefunden!")
        print("Setze sie als Argument: --railway-url 'postgresql://...'")
        print("Oder als Umgebungsvariable: export DATABASE_URL='postgresql://...'")
        return
    
    # Teste Railway PostgreSQL Verbindung
    try:
        test_conn = psycopg2.connect(railway_url)
        test_conn.close()
        print("✅ Railway PostgreSQL Verbindung erfolgreich")
    except Exception as e:
        print(f"❌ Railway PostgreSQL Verbindung fehlgeschlagen: {e}")
        return
    
    # Führe Migration durch
    migrate_chunks_data(args.duckdb, railway_url)

if __name__ == "__main__":
    main()
