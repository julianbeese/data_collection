#!/usr/bin/env python3
"""
Streamlit App für Brexit-Datenanalyse
Ermöglicht Exploration der Brexit-Debatten-Datenbank
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os

# Datenbank-Pfad
DB_FILE = "data/processed/debates_brexit_filtered.duckdb"

def get_db_connection():
    """Erstellt DuckDB Verbindung"""
    try:
        if not os.path.exists(DB_FILE):
            st.error(f"Datenbank nicht gefunden: {DB_FILE}")
            return None
        return duckdb.connect(DB_FILE, read_only=True)
    except Exception as e:
        st.error(f"Fehler bei Datenbankverbindung: {e}")
        return None

def load_data_overview(conn):
    """Lädt Datenbank-Übersicht"""
    try:
        # Anzahl Debatten
        debates_count = conn.execute("SELECT COUNT(*) FROM debates").fetchone()[0]
        
        # Anzahl Reden
        speeches_count = conn.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
        
        # Zeitraum
        date_range = conn.execute("""
            SELECT MIN(date) as start_date, MAX(date) as end_date 
            FROM debates
        """).fetchone()
        
        return {
            'debates': debates_count,
            'speeches': speeches_count,
            'start_date': date_range[0],
            'end_date': date_range[1]
        }
    except Exception as e:
        st.error(f"Fehler beim Laden der Übersicht: {e}")
        return None

def show_speaker_statistics(conn):
    """Zeigt Sprecher-Statistiken"""
    try:
        # Top 10 Sprecher
        speaker_stats = conn.execute("""
            SELECT speaker, COUNT(*) as speech_count
            FROM speeches 
            WHERE speaker IS NOT NULL
            GROUP BY speaker 
            ORDER BY speech_count DESC 
            LIMIT 10
        """).fetchdf()
        
        if not speaker_stats.empty:
            fig = px.bar(
                speaker_stats, 
                x='speech_count', 
                y='speaker',
                orientation='h',
                title="Top 10 Sprecher (Anzahl Reden)"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        return speaker_stats
    except Exception as e:
        st.error(f"Fehler bei Sprecher-Statistiken: {e}")
        return None

def show_timeline_analysis(conn):
    """Zeigt zeitliche Entwicklung"""
    try:
        # Reden pro Monat
        monthly_stats = conn.execute("""
            SELECT 
                DATE_TRUNC('month', date) as month,
                COUNT(*) as speech_count
            FROM speeches s
            JOIN debates d ON s.debate_id = d.debate_id
            GROUP BY month
            ORDER BY month
        """).fetchdf()
        
        if not monthly_stats.empty:
            fig = px.line(
                monthly_stats,
                x='month',
                y='speech_count',
                title="Brexit-Reden pro Monat"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        return monthly_stats
    except Exception as e:
        st.error(f"Fehler bei Timeline-Analyse: {e}")
        return None

def search_speeches(conn, search_term, limit=50):
    """Sucht Reden nach Suchbegriff"""
    try:
        search_query = f"""
            SELECT s.speech_id, s.speaker, s.text, d.date, d.major_heading_text
            FROM speeches s
            JOIN debates d ON s.debate_id = d.debate_id
            WHERE LOWER(s.text) LIKE LOWER('%{search_term}%')
            ORDER BY d.date DESC
            LIMIT {limit}
        """
        
        results = conn.execute(search_query).fetchdf()
        return results
    except Exception as e:
        st.error(f"Fehler bei der Suche: {e}")
        return None

def main():
    st.set_page_config(
        page_title="Brexit Data Analysis",
        page_icon="🇬🇧",
        layout="wide"
    )
    
    st.title("🇬🇧 Brexit Data Analysis")
    st.markdown("**Analyse der Brexit-Debatten aus dem britischen Unterhaus**")
    
    # Datenbankverbindung
    conn = get_db_connection()
    if not conn:
        st.error("Keine Datenbankverbindung möglich!")
        return
    
    # Sidebar
    with st.sidebar:
        st.header("🔍 Navigation")
        page = st.selectbox(
            "Seite auswählen:",
            ["Übersicht", "Sprecher-Analyse", "Timeline", "Suche"]
        )
    
    # Hauptinhalt
    if page == "Übersicht":
        st.header("📊 Datenbank-Übersicht")
        
        overview = load_data_overview(conn)
        if overview:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Debatten", f"{overview['debates']:,}")
            
            with col2:
                st.metric("Reden", f"{overview['speeches']:,}")
            
            with col3:
                st.metric("Zeitraum", f"{overview['start_date']} bis {overview['end_date']}")
    
    elif page == "Sprecher-Analyse":
        st.header("👥 Sprecher-Statistiken")
        show_speaker_statistics(conn)
    
    elif page == "Timeline":
        st.header("📈 Zeitliche Entwicklung")
        show_timeline_analysis(conn)
    
    elif page == "Suche":
        st.header("🔍 Rede-Suche")
        
        search_term = st.text_input("Suchbegriff eingeben:")
        limit = st.slider("Maximale Ergebnisse:", 10, 100, 50)
        
        if st.button("Suchen") and search_term:
            with st.spinner("Suche läuft..."):
                results = search_speeches(conn, search_term, limit)
                
                if results is not None and not results.empty:
                    st.success(f"✓ {len(results)} Ergebnisse gefunden")
                    
                    for idx, row in results.iterrows():
                        with st.expander(f"🎤 {row['speaker']} - {row['date']}"):
                            st.write(f"**Thema:** {row['major_heading_text']}")
                            st.write(f"**Text:** {row['text'][:500]}...")
                else:
                    st.warning("Keine Ergebnisse gefunden")
    
    # Footer
    st.divider()
    st.markdown("**Datenquelle:** TheyWorkForYou | **Datenbank:** DuckDB")

if __name__ == "__main__":
    main()
