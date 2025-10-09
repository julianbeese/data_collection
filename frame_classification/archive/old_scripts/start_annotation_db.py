#!/usr/bin/env python3
"""
Start-Script für das Datenbank-basierte Streamlit Annotation Interface
"""

import subprocess
import sys
from pathlib import Path

def main():
    print("🚀 Starte Streamlit Annotation Interface (Database)...")
    print("=" * 60)
    
    # Prüfe ob Streamlit installiert ist
    try:
        import streamlit
        print("✅ Streamlit ist installiert")
    except ImportError:
        print("❌ Streamlit nicht installiert!")
        print("Installiere mit: pip install streamlit")
        return
    
    # Prüfe ob Datenbank existiert
    db_path = Path("../data/processed/debates_brexit_chunked.duckdb")
    if not db_path.exists():
        print("❌ Datenbank nicht gefunden!")
        print("Führe zuerst aus: python scripts/simple_database_chunking.py")
        return
    
    print("✅ Datenbank gefunden")
    print("🌐 Öffne Browser und gehe zu: http://localhost:8501")
    print("💡 Drücke Ctrl+C zum Beenden")
    print("=" * 60)
    
    # Starte Streamlit
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit_annotation_db.py",
            "--server.port", "8501",
            "--server.headless", "true"
        ])
    except KeyboardInterrupt:
        print("\n👋 Streamlit beendet")

if __name__ == "__main__":
    main()

