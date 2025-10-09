#!/usr/bin/env python3
"""
Optimierte Migration: DuckDB zu Railway PostgreSQL
Direkte Verbindung und Batch-Inserts für maximale Performance
"""

import duckdb
import psycopg2
from psycopg2.extras import execute_batch, execute_values
import pandas as pd
import argparse
from pathlib import Path
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import numpy as np
from dotenv import load_dotenv

# Lade Umgebungsvariablen
load_dotenv()

class OptimizedRailwayMigration:
    def __init__(self, duckdb_path: str, postgres_url: Optional[str] = None):
        self.duckdb_path = duckdb_path
        self.postgres_url = postgres_url or os.getenv('DATABASE_URL')
        
        if not self.postgres_url:
            raise ValueError("PostgreSQL URL benötigt! Setze DATABASE_URL oder nutze --postgres-url")
        
        # Performance-Einstellungen
        self.batch_size = 5000  # Größere Batches für bessere Performance
        self.page_size = 1000   # Für execute_batch
        
    def connect_postgres(self):
        """Erstellt optimierte PostgreSQL-Verbindung"""
        conn = psycopg2.connect(self.postgres_url)
        conn.set_session(autocommit=False)  # Für Transaktionen
        
        # Performance-Optimierungen
        cur = conn.cursor()
        cur.execute("SET synchronous_commit = OFF")  # Schnellere Writes
        cur.execute("SET maintenance_work_mem = '256MB'")  # Mehr RAM für Indizes
        cur.execute("SET work_mem = '256MB'")  # Mehr RAM für Sortierung
        cur.close()
        
        return conn
    
    def create_tables_optimized(self, conn):
        """Erstellt Tabellen mit optimierten Einstellungen"""
        print("📦 Erstelle optimierte Tabellen...")
        
        cur = conn.cursor()
        
        # Droppe alte Tabellen für sauberen Start (optional)
        cur.execute("DROP TABLE IF EXISTS chunks CASCADE")
        cur.execute("DROP TABLE IF EXISTS agreement_chunks CASCADE")
        
        # Chunks-Tabelle ohne Indizes (werden später erstellt)
        cur.execute("""
            CREATE TABLE chunks (
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
            ) WITH (autovacuum_enabled = false)  -- Deaktiviere während Import
        """)
        
        # Agreement-Tabelle
        cur.execute("""
            CREATE TABLE agreement_chunks (
                chunk_id VARCHAR(255) PRIMARY KEY,
                annotator1 VARCHAR(255),
                annotator2 VARCHAR(255),
                label1 VARCHAR(100),
                label2 VARCHAR(100),
                agreement_score DECIMAL(3,2),
                agreement_perfect BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) WITH (autovacuum_enabled = false)
        """)
        
        conn.commit()
        cur.close()
        print("✅ Tabellen erstellt")
    
    def migrate_chunks_direct(self):
        """Direkte Migration mit COPY-Performance"""
        print("\n🚀 Starte direkte Migration...")
        start_time = time.time()
        
        # PostgreSQL-Verbindung
        pg_conn = self.connect_postgres()
        self.create_tables_optimized(pg_conn)
        
        # DuckDB-Verbindung
        duck_conn = duckdb.connect(self.duckdb_path, read_only=True)
        
        try:
            # Hole Chunk-Anzahl
            total_chunks = duck_conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
            print(f"📊 Migriere {total_chunks:,} Chunks...")
            
            # Nutze DuckDB's COPY TO für Export und PostgreSQL's COPY FROM
            self._migrate_with_copy(duck_conn, pg_conn)
            
            # Migriere Agreement-Tabelle falls vorhanden
            try:
                agreement_count = duck_conn.execute("SELECT COUNT(*) FROM agreement_chunks").fetchone()[0]
                if agreement_count > 0:
                    print(f"\n📊 Migriere {agreement_count:,} Agreement-Einträge...")
                    self._migrate_agreement_with_copy(duck_conn, pg_conn)
            except:
                print("ℹ️ Keine Agreement-Tabelle gefunden")
            
            # Erstelle Indizes NACH dem Import (viel schneller!)
            self._create_indexes_after_import(pg_conn)
            
            # Aktiviere Autovacuum wieder
            self._enable_autovacuum(pg_conn)
            
            elapsed = time.time() - start_time
            print(f"\n✅ Migration abgeschlossen in {elapsed:.1f} Sekunden")
            print(f"⚡ Performance: {total_chunks/elapsed:.0f} Chunks/Sekunde")
            
        except Exception as e:
            pg_conn.rollback()
            print(f"❌ Fehler bei Migration: {e}")
            raise
        finally:
            duck_conn.close()
            pg_conn.close()
    
    def _migrate_with_copy(self, duck_conn, pg_conn):
        """Nutzt COPY für maximale Performance"""
        import tempfile
        import csv
        
        # Erstelle temporäre CSV-Datei
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
            
            # Exportiere aus DuckDB
            print("  📤 Exportiere aus DuckDB...")
            duck_conn.execute(f"""
                COPY (
                    SELECT 
                        chunk_id, speech_id, debate_id, speaker_name, speaker_party,
                        debate_title, debate_date, chunk_text, chunk_index, total_chunks,
                        word_count, char_count, chunking_method, assigned_user,
                        frame_label, annotation_confidence, annotation_notes
                    FROM chunks
                    ORDER BY chunk_id
                ) TO '{tmp_path}' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """)
            
        # Importiere in PostgreSQL mit COPY
        print("  📥 Importiere in PostgreSQL...")
        with open(tmp_path, 'r') as f:
            cur = pg_conn.cursor()
            cur.copy_expert(
                sql="""
                    COPY chunks (
                        chunk_id, speech_id, debate_id, speaker_name, speaker_party,
                        debate_title, debate_date, chunk_text, chunk_index, total_chunks,
                        word_count, char_count, chunking_method, assigned_user,
                        frame_label, annotation_confidence, annotation_notes
                    ) FROM STDIN WITH CSV HEADER
                """,
                file=f
            )
            pg_conn.commit()
            cur.close()
        
        # Lösche temporäre Datei
        os.unlink(tmp_path)
        print("  ✅ Chunks migriert")
    
    def _migrate_agreement_with_copy(self, duck_conn, pg_conn):
        """Migriert Agreement-Tabelle mit COPY"""
        import tempfile
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
            
            duck_conn.execute(f"""
                COPY (
                    SELECT chunk_id, annotator1, annotator2, label1, label2,
                           agreement_score, agreement_perfect
                    FROM agreement_chunks
                ) TO '{tmp_path}' WITH (FORMAT CSV, HEADER TRUE)
            """)
        
        with open(tmp_path, 'r') as f:
            cur = pg_conn.cursor()
            cur.copy_expert(
                sql="""
                    COPY agreement_chunks (
                        chunk_id, annotator1, annotator2, label1, label2,
                        agreement_score, agreement_perfect
                    ) FROM STDIN WITH CSV HEADER
                """,
                file=f
            )
            pg_conn.commit()
            cur.close()
        
        os.unlink(tmp_path)
        print("  ✅ Agreement-Daten migriert")
    
    def _create_indexes_after_import(self, conn):
        """Erstellt Indizes NACH dem Import für bessere Performance"""
        print("\n🔧 Erstelle Indizes...")
        start_time = time.time()
        
        cur = conn.cursor()
        
        indexes = [
            ("idx_chunks_assigned_user", "chunks(assigned_user)"),
            ("idx_chunks_frame_label", "chunks(frame_label)"),
            ("idx_chunks_speaker", "chunks(speaker_name)"),
            ("idx_chunks_debate", "chunks(debate_id)"),
            ("idx_chunks_speech", "chunks(speech_id)"),
            ("idx_agreement_annotator1", "agreement_chunks(annotator1)"),
            ("idx_agreement_annotator2", "agreement_chunks(annotator2)"),
        ]
        
        for idx_name, idx_def in indexes:
            print(f"  📍 Erstelle {idx_name}...")
            cur.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {idx_def}")
        
        # Analysiere Tabellen für Statistiken
        cur.execute("ANALYZE chunks")
        cur.execute("ANALYZE agreement_chunks")
        
        conn.commit()
        cur.close()
        
        elapsed = time.time() - start_time
        print(f"  ✅ Indizes erstellt in {elapsed:.1f} Sekunden")
    
    def _enable_autovacuum(self, conn):
        """Aktiviert Autovacuum wieder nach Import"""
        cur = conn.cursor()
        cur.execute("ALTER TABLE chunks SET (autovacuum_enabled = true)")
        cur.execute("ALTER TABLE agreement_chunks SET (autovacuum_enabled = true)")
        conn.commit()
        cur.close()
    
    def verify_migration(self):
        """Verifiziert die Migration"""
        print("\n🔍 Verifiziere Migration...")
        
        # DuckDB Counts
        duck_conn = duckdb.connect(self.duckdb_path, read_only=True)
        duck_chunks = duck_conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        
        try:
            duck_agreement = duck_conn.execute("SELECT COUNT(*) FROM agreement_chunks").fetchone()[0]
        except:
            duck_agreement = 0
        
        duck_conn.close()
        
        # PostgreSQL Counts
        pg_conn = psycopg2.connect(self.postgres_url)
        cur = pg_conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM chunks")
        pg_chunks = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM agreement_chunks")
        pg_agreement = cur.fetchone()[0]
        
        # Detaillierte Statistiken
        cur.execute("""
            SELECT 
                COUNT(DISTINCT assigned_user) as users,
                COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as labeled,
                COUNT(CASE WHEN assigned_user IS NOT NULL THEN 1 END) as assigned
            FROM chunks
        """)
        stats = cur.fetchone()
        
        cur.close()
        pg_conn.close()
        
        # Vergleiche
        print(f"\n📊 Migrations-Verifikation:")
        print(f"  Chunks:     DuckDB: {duck_chunks:,} | PostgreSQL: {pg_chunks:,} ", end="")
        print("✅" if duck_chunks == pg_chunks else "❌")
        
        print(f"  Agreement:  DuckDB: {duck_agreement:,} | PostgreSQL: {pg_agreement:,} ", end="")
        print("✅" if duck_agreement == pg_agreement else "❌")
        
        print(f"\n📈 Datenbank-Statistiken:")
        print(f"  Annotator-Users:     {stats[0]}")
        print(f"  Gelabelte Chunks:    {stats[1]:,}")
        print(f"  Zugewiesene Chunks:  {stats[2]:,}")
        
        return duck_chunks == pg_chunks

def migrate_with_fallback(duckdb_path: str, postgres_url: Optional[str] = None):
    """Migration mit Fallback zu CSV wenn direkte Migration fehlschlägt"""
    
    migrator = OptimizedRailwayMigration(duckdb_path, postgres_url)
    
    try:
        # Versuche direkte Migration (schnellste Methode)
        migrator.migrate_chunks_direct()
        
        # Verifiziere
        if migrator.verify_migration():
            print("\n🎉 Migration erfolgreich!")
            return True
        else:
            print("\n⚠️ Verifikation fehlgeschlagen!")
            return False
            
    except Exception as e:
        print(f"\n❌ Direkte Migration fehlgeschlagen: {e}")
        print("💡 Tipp: Stelle sicher, dass DATABASE_URL gesetzt ist oder nutze --postgres-url")
        
        # Fallback zu CSV-Export
        print("\n📦 Fallback: Exportiere zu CSV für manuellen Import...")
        export_to_csv_optimized(duckdb_path, "railway_export")
        return False

def export_to_csv_optimized(duckdb_path: str, output_dir: str):
    """Optimierter CSV-Export als Fallback"""
    Path(output_dir).mkdir(exist_ok=True)
    
    duck_conn = duckdb.connect(duckdb_path, read_only=True)
    
    # Nutze DuckDB's native CSV-Export (sehr schnell!)
    print("  📤 Exportiere Chunks...")
    duck_conn.execute(f"""
        COPY chunks 
        TO '{output_dir}/chunks.csv' 
        WITH (FORMAT CSV, HEADER TRUE)
    """)
    
    try:
        duck_conn.execute(f"""
            COPY agreement_chunks 
            TO '{output_dir}/agreement_chunks.csv' 
            WITH (FORMAT CSV, HEADER TRUE)
        """)
        print("  📤 Exportiere Agreement...")
    except:
        pass
    
    duck_conn.close()
    
    # Erstelle Import-Script
    with open(f"{output_dir}/import.sql", 'w') as f:
        f.write("""
-- Schneller PostgreSQL Import
-- 1. Lade CSV-Dateien zu Railway hoch
-- 2. Führe dieses Script aus

-- Deaktiviere Autovacuum während Import
ALTER TABLE chunks SET (autovacuum_enabled = false);
ALTER TABLE agreement_chunks SET (autovacuum_enabled = false);

-- Import mit COPY (schnellste Methode)
\\COPY chunks FROM 'chunks.csv' WITH CSV HEADER;
\\COPY agreement_chunks FROM 'agreement_chunks.csv' WITH CSV HEADER;

-- Erstelle Indizes NACH Import
CREATE INDEX CONCURRENTLY idx_chunks_assigned_user ON chunks(assigned_user);
CREATE INDEX CONCURRENTLY idx_chunks_frame_label ON chunks(frame_label);
CREATE INDEX CONCURRENTLY idx_chunks_speaker ON chunks(speaker_name);
CREATE INDEX CONCURRENTLY idx_chunks_debate ON chunks(debate_id);

-- Aktiviere Autovacuum wieder
ALTER TABLE chunks SET (autovacuum_enabled = true);
ALTER TABLE agreement_chunks SET (autovacuum_enabled = true);

-- Analysiere für Statistiken
ANALYZE chunks;
ANALYZE agreement_chunks;
        """)
    
    print(f"\n✅ CSV-Export abgeschlossen: {output_dir}/")
    print("📝 Nutze import.sql für schnellen Import in Railway")

def main():
    parser = argparse.ArgumentParser(description="Optimierte DuckDB zu Railway PostgreSQL Migration")
    parser.add_argument("--duckdb", required=True, help="Pfad zur DuckDB-Datei")
    parser.add_argument("--postgres-url", help="PostgreSQL URL (oder setze DATABASE_URL)")
    parser.add_argument("--method", choices=["direct", "csv"], default="direct", 
                       help="Migration-Methode: direct (schnell) oder csv (fallback)")
    
    args = parser.parse_args()
    
    # Prüfe DuckDB
    if not Path(args.duckdb).exists():
        print(f"❌ DuckDB nicht gefunden: {args.duckdb}")
        return
    
    print("=" * 70)
    print("🚀 OPTIMIERTE RAILWAY MIGRATION")
    print("=" * 70)
    
    if args.method == "direct":
        # Direkte Migration (empfohlen)
        migrate_with_fallback(args.duckdb, args.postgres_url)
    else:
        # CSV-Export
        export_to_csv_optimized(args.duckdb, "railway_export")
    
    print("\n📌 Nächste Schritte:")
    print("1. Stelle sicher, dass die Daten korrekt migriert wurden")
    print("2. Teste die Annotation-App mit der Railway-Datenbank")
    print("3. Überwache die Performance mit Railway Metrics")

if __name__ == "__main__":
    main()