#!/usr/bin/env python3
"""
Einfacher Export von DuckDB zu CSV
"""

import duckdb
import csv
import argparse
from pathlib import Path

def export_to_csv(duckdb_path: str, output_dir: str):
    """Exportiert DuckDB zu CSV"""
    
    print("🔄 Exportiere DuckDB zu CSV...")
    Path(output_dir).mkdir(exist_ok=True)
    
    conn = duckdb.connect(duckdb_path, read_only=True)
    
    try:
        # Exportiere Chunks
        print("📥 Exportiere Chunks...")
        chunks_data = conn.execute("SELECT * FROM chunks").fetchall()
        chunks_columns = [desc[0] for desc in conn.execute("DESCRIBE chunks").fetchall()]
        
        chunks_csv = Path(output_dir) / "chunks.csv"
        with open(chunks_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(chunks_columns)
            writer.writerows(chunks_data)
        
        print(f"✅ Chunks exportiert: {chunks_csv} ({len(chunks_data)} Zeilen)")
        
        # Exportiere Agreement (falls vorhanden)
        try:
            agreement_data = conn.execute("SELECT * FROM agreement_chunks").fetchall()
            if agreement_data:
                agreement_columns = [desc[0] for desc in conn.execute("DESCRIBE agreement_chunks").fetchall()]
                agreement_csv = Path(output_dir) / "agreement_chunks.csv"
                with open(agreement_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(agreement_columns)
                    writer.writerows(agreement_data)
                print(f"✅ Agreement exportiert: {agreement_csv} ({len(agreement_data)} Zeilen)")
            else:
                print("⚠️ Keine Agreement-Daten gefunden")
        except Exception as e:
            print(f"⚠️ Agreement-Tabelle nicht gefunden: {e}")
        
        # Statistiken
        annotated_count = sum(1 for row in chunks_data if row[14] is not None)  # frame_label
        assigned_count = sum(1 for row in chunks_data if row[13] is not None and row[13] != '')  # assigned_user
        
        print(f"\n📊 Export-Statistiken:")
        print(f"  📝 Gesamt Chunks: {len(chunks_data):,}")
        print(f"  🏷️ Annotierte Chunks: {annotated_count:,}")
        print(f"  👥 Zugewiesene Chunks: {assigned_count:,}")
        
    except Exception as e:
        print(f"❌ Fehler beim Export: {e}")
        raise
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Exportiere DuckDB zu CSV")
    parser.add_argument("--duckdb", required=True, help="Pfad zur DuckDB-Datei")
    parser.add_argument("--output-dir", default="railway_export", help="Output-Verzeichnis")
    
    args = parser.parse_args()
    
    if not Path(args.duckdb).exists():
        print(f"❌ DuckDB-Datei nicht gefunden: {args.duckdb}")
        return
    
    export_to_csv(args.duckdb, args.output_dir)
    
    print(f"\n🎯 Nächste Schritte:")
    print(f"1. Lade die CSV-Dateien zu Railway hoch")
    print(f"2. Nutze Railway Dashboard für den Import")
    print(f"3. Oder nutze pgAdmin/psql für den Import")

if __name__ == "__main__":
    main()
