#!/usr/bin/env python3
"""
Streamlit Annotation Interface für Railway Deployment
Optimiert für Railway PostgreSQL und Multi-User-Zugriff
"""

import streamlit as st
import psycopg2
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import plotly.express as px
import plotly.graph_objects as go
import os
from psycopg2.extras import RealDictCursor

# Railway PostgreSQL Konfiguration (Fallback für lokale Entwicklung)
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'frame_classification'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Frame-Kategorien
FRAME_CATEGORIES = [
    "Human Impact",
    "Powerlessness", 
    "Economic",
    "Moral Value",
    "Conflict",
    "None/Not Relevant",
    "t.b.d."
]

# Brexit-Position Kategorien
BREXIT_POSITION_CATEGORIES = [
    "Pro-Brexit",
    "Anti-Brexit", 
    "Neutral/Unclear",
    "Not Applicable"
]

def get_db_connection():
    """Erstellt PostgreSQL Verbindung für Railway"""
    try:
        # Railway DATABASE_URL Format: postgresql://user:password@host:port/database
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            # Verwende die DATABASE_URL direkt
            conn = psycopg2.connect(database_url)
            return conn
        else:
            # Fallback für lokale Entwicklung
            conn = psycopg2.connect(**DATABASE_CONFIG)
            return conn
    except Exception as e:
        st.error(f"Fehler bei Datenbankverbindung: {e}")
        st.error(f"DATABASE_URL: {os.getenv('DATABASE_URL', 'Nicht gesetzt')}")
        return None

def init_session_state():
    """Initialisiert Session State"""
    if 'chunks' not in st.session_state:
        st.session_state.chunks = []
    if 'current_chunk_index' not in st.session_state:
        st.session_state.current_chunk_index = 0
    if 'annotations' not in st.session_state:
        st.session_state.annotations = {}
    if 'user_name' not in st.session_state:
        st.session_state.user_name = ""
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None

def create_tables_if_not_exist():
    """Erstellt Tabellen falls sie nicht existieren"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
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
                pre_brexit BOOLEAN,
                brexit_position VARCHAR(100),
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
        
        # Annotation-History-Tabelle für echte Konflikte
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS annotation_history (
                id SERIAL PRIMARY KEY,
                chunk_id VARCHAR(255),
                user_name VARCHAR(255),
                frame_label VARCHAR(100),
                brexit_position VARCHAR(100),
                annotation_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id)
            );
        """)
        
        # Indizes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_assigned_user ON chunks(assigned_user);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chunks_frame_label ON chunks(frame_label);")
        
        # Füge neue Spalten hinzu falls sie nicht existieren
        try:
            cursor.execute("ALTER TABLE chunks ADD COLUMN IF NOT EXISTS pre_brexit BOOLEAN;")
            cursor.execute("ALTER TABLE chunks ADD COLUMN IF NOT EXISTS brexit_position VARCHAR(100);")
        except Exception as e:
            # Spalten existieren möglicherweise bereits
            pass
        
        # Aktualisiere pre_brexit Spalte basierend auf debate_date
        cursor.execute("""
            UPDATE chunks 
            SET pre_brexit = (debate_date < '2016-06-23'::date)
            WHERE pre_brexit IS NULL
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"Fehler beim Erstellen der Tabellen: {e}")
        if conn:
            conn.close()

def load_database_chunks(user_name: str = None, limit: int = None, only_unannotated: bool = True) -> List[Dict[str, Any]]:
    """Lädt Chunks aus PostgreSQL für einen bestimmten User"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if user_name:
            if only_unannotated:
                # Lade nur unklassifizierte Chunks für den spezifischen User
                if limit:
                    query = """
                    SELECT * FROM chunks 
                    WHERE assigned_user = %s 
                    AND (frame_label IS NULL OR frame_label = '')
                    ORDER BY chunk_id 
                    LIMIT %s
                    """
                    cursor.execute(query, (user_name, limit))
                else:
                    query = """
                    SELECT * FROM chunks 
                    WHERE assigned_user = %s 
                    AND (frame_label IS NULL OR frame_label = '')
                    ORDER BY chunk_id
                    """
                    cursor.execute(query, (user_name,))
            else:
                # Lade alle Chunks für den spezifischen User (inkl. klassifizierte)
                if limit:
                    query = """
                    SELECT * FROM chunks 
                    WHERE assigned_user = %s
                    ORDER BY chunk_id 
                    LIMIT %s
                    """
                    cursor.execute(query, (user_name, limit))
                else:
                    query = """
                    SELECT * FROM chunks 
                    WHERE assigned_user = %s
                    ORDER BY chunk_id
                    """
                    cursor.execute(query, (user_name,))
        else:
            # Lade alle unzugewiesenen Chunks (für Admin-View)
            if limit:
                query = """
                SELECT * FROM chunks 
                WHERE assigned_user IS NULL OR assigned_user = ''
                ORDER BY chunk_id 
                LIMIT %s
                """
                cursor.execute(query, (limit,))
            else:
                query = """
                SELECT * FROM chunks 
                WHERE assigned_user IS NULL OR assigned_user = ''
                ORDER BY chunk_id
                """
                cursor.execute(query)
        
        chunks = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Konvertiere zu Dictionary-Liste
        chunk_list = []
        for chunk in chunks:
            chunk_dict = dict(chunk)
            chunk_list.append(chunk_dict)
        
        return chunk_list
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Chunks: {e}")
        if conn:
            conn.close()
        return []

def update_database_annotation(chunk_id: str, frame_label: str, confidence: int, notes: str, user_name: str, brexit_position: str = None):
    """Aktualisiert Annotation in PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Aktualisiere chunks-Tabelle
        update_sql = """
        UPDATE chunks 
        SET frame_label = %s, annotation_confidence = %s, annotation_notes = %s, 
            assigned_user = %s, brexit_position = %s, updated_at = CURRENT_TIMESTAMP
        WHERE chunk_id = %s
        """
        
        cursor.execute(update_sql, (frame_label, confidence, notes, user_name, brexit_position, chunk_id))
        
        # Füge auch zur Annotation-History hinzu
        history_sql = """
        INSERT INTO annotation_history (chunk_id, user_name, frame_label, brexit_position, annotation_notes)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(history_sql, (chunk_id, user_name, frame_label, brexit_position, notes))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"Fehler beim Aktualisieren der Datenbank: {e}")
        if conn:
            conn.rollback()
            conn.close()

def get_statistics() -> Dict[str, Any]:
    """Berechnet Statistiken"""
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        
        # Gesamt-Statistiken
        cursor.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks,
                COUNT(CASE WHEN assigned_user IS NOT NULL AND assigned_user != '' THEN 1 END) as assigned_chunks
            FROM chunks
        """)
        total_stats = cursor.fetchone()
        
        # Frame-Verteilung
        cursor.execute("""
            SELECT frame_label, COUNT(*) as count
            FROM chunks 
            WHERE frame_label IS NOT NULL
            GROUP BY frame_label
            ORDER BY count DESC
        """)
        frame_stats = cursor.fetchall()
        
        # User-Statistiken
        cursor.execute("""
            SELECT assigned_user, COUNT(*) as count
            FROM chunks 
            WHERE assigned_user IS NOT NULL AND assigned_user != ''
            GROUP BY assigned_user
            ORDER BY count DESC
        """)
        user_stats = cursor.fetchall()
        
        # Brexit-Position Statistiken
        cursor.execute("""
            SELECT brexit_position, COUNT(*) as count
            FROM chunks 
            WHERE brexit_position IS NOT NULL AND brexit_position != ''
            GROUP BY brexit_position
            ORDER BY count DESC
        """)
        brexit_stats = cursor.fetchall()
        
        # Pre-Brexit vs Post-Brexit Statistiken
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN pre_brexit = true THEN 1 END) as pre_brexit_count,
                COUNT(CASE WHEN pre_brexit = false THEN 1 END) as post_brexit_count
            FROM chunks
        """)
        brexit_timing_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            'total_chunks': total_stats[0],
            'annotated_chunks': total_stats[1],
            'assigned_chunks': total_stats[2],
            'by_frame': {frame: count for frame, count in frame_stats},
            'by_user': {user: count for user, count in user_stats},
            'by_brexit_position': {position: count for position, count in brexit_stats},
            'pre_brexit_count': brexit_timing_stats[0],
            'post_brexit_count': brexit_timing_stats[1]
        }
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Statistiken: {e}")
        if conn:
            conn.close()
        return {}

def show_statistics():
    """Zeigt Statistiken"""
    stats = get_statistics()
    
    if not stats:
        return
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Gesamt Chunks", f"{stats['total_chunks']:,}")
    
    with col2:
        st.metric("Annotiert", f"{stats['annotated_chunks']:,}")
    
    with col3:
        st.metric("Zugewiesen", f"{stats['assigned_chunks']:,}")
    
    # Frame-Verteilung
    if stats['by_frame']:
        st.subheader("📊 Frame-Verteilung")
        
        try:
            frame_data = []
            for frame, count in stats['by_frame'].items():
                frame_data.append({'Frame': str(frame), 'Anzahl': int(count)})
            
            if frame_data:
                frame_df = pd.DataFrame(frame_data)
                st.bar_chart(frame_df.set_index('Frame'))
            else:
                st.info("Keine Frame-Daten verfügbar")
        except Exception as e:
            st.warning(f"Konnte Chart nicht anzeigen: {e}")
            st.write("**Frame-Verteilung:**")
            for frame, count in stats['by_frame'].items():
                st.write(f"- {frame}: {count}")
    
    # User-Verteilung
    if stats['by_user']:
        st.subheader("👥 User-Verteilung")
        for user, count in stats['by_user'].items():
            st.write(f"- **{user}**: {count:,} Chunks")
    
    # Brexit-Timing Verteilung
    if 'pre_brexit_count' in stats and 'post_brexit_count' in stats:
        st.subheader("📅 Brexit-Timing")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Pre-Brexit Chunks", f"{stats['pre_brexit_count']:,}")
        with col2:
            st.metric("Post-Brexit Chunks", f"{stats['post_brexit_count']:,}")
    
    # Brexit-Position Verteilung
    if stats.get('by_brexit_position'):
        st.subheader("🇬🇧 Brexit-Positionen")
        
        try:
            brexit_data = []
            for position, count in stats['by_brexit_position'].items():
                brexit_data.append({'Position': str(position), 'Anzahl': int(count)})
            
            if brexit_data:
                brexit_df = pd.DataFrame(brexit_data)
                st.bar_chart(brexit_df.set_index('Position'))
            else:
                st.info("Keine Brexit-Position Daten verfügbar")
        except Exception as e:
            st.warning(f"Konnte Chart nicht anzeigen: {e}")
            st.write("**Brexit-Positionen:**")
            for position, count in stats['by_brexit_position'].items():
                st.write(f"- {position}: {count}")

def show_chunk_annotation():
    """Zeigt Chunk-Annotation Interface"""
    if not st.session_state.chunks:
        st.warning("Keine Chunks geladen!")
        return
    
    current_chunk = st.session_state.chunks[st.session_state.current_chunk_index]
    chunk_id = current_chunk['chunk_id']
    
    # Chunk-Informationen
    st.subheader(f"📝 Chunk {st.session_state.current_chunk_index + 1} von {len(st.session_state.chunks)}")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write(f"**ID:** {chunk_id}")
    with col2:
        st.write(f"**Wörter:** {current_chunk['word_count']}")
    with col3:
        st.write(f"**Zeichen:** {current_chunk['char_count']}")
    
    # Speaker-Informationen
    st.write(f"**Speaker:** {current_chunk['speaker_name']} ({current_chunk['speaker_party']})")
    st.write(f"**Debatte:** {current_chunk['debate_title']}")
    st.write(f"**Datum:** {current_chunk['debate_date']}")
    
    # Pre-Brexit Status
    pre_brexit = current_chunk.get('pre_brexit', False)
    if pre_brexit:
        st.info("🇬🇧 **Pre-Brexit Chunk** - Dieser Chunk stammt aus der Zeit vor dem Brexit-Referendum (23. Juni 2016)")
    else:
        st.info("📅 **Post-Brexit Chunk** - Dieser Chunk stammt aus der Zeit nach dem Brexit-Referendum")
    
    # Chunk-Text
    st.subheader("📄 Chunk-Text")
    st.text_area("", current_chunk['chunk_text'], height=200, disabled=True)
    
    # Annotation-Formular
    st.subheader("🏷️ Frame-Annotation")
    
    # Frame-Auswahl
    frame_label = st.selectbox(
        "Frame-Kategorie:",
        options=[""] + FRAME_CATEGORIES,
        key=f"frame_{chunk_id}"
    )
    
    # Brexit-Position (nur für Pre-Brexit Chunks)
    brexit_position = None
    if pre_brexit:
        st.subheader("🇬🇧 Brexit-Position")
        brexit_position = st.selectbox(
            "Position des Sprechers zum Brexit:",
            options=[""] + BREXIT_POSITION_CATEGORIES,
            key=f"brexit_{chunk_id}",
            help="Nur für Pre-Brexit Chunks relevant"
        )
    
    # Notes
    notes = st.text_area(
        "Notizen:",
        placeholder="Optionale Notizen zur Annotation...",
        key=f"notes_{chunk_id}"
    )
    
    # Buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("💾 Speichern", type="primary"):
            if frame_label:
                # Prüfe ob Brexit-Position für Pre-Brexit Chunks erforderlich ist
                if pre_brexit and not brexit_position:
                    st.error("Bitte wähle eine Brexit-Position für Pre-Brexit Chunks!")
                    return
                
                # Aktualisiere Datenbank
                update_database_annotation(
                    chunk_id, frame_label, 3, notes, st.session_state.user_name, brexit_position
                )
                
                st.success("✅ Annotation gespeichert!")
                
                # Automatisch zum nächsten Chunk
                if st.session_state.current_chunk_index < len(st.session_state.chunks) - 1:
                    st.session_state.current_chunk_index += 1
                    # Formularfelder zurücksetzen
                    if f"frame_{chunk_id}" in st.session_state:
                        del st.session_state[f"frame_{chunk_id}"]
                    if f"brexit_{chunk_id}" in st.session_state:
                        del st.session_state[f"brexit_{chunk_id}"]
                    if f"notes_{chunk_id}" in st.session_state:
                        del st.session_state[f"notes_{chunk_id}"]
                    st.info("🔄 Lade nächsten Chunk...")
                else:
                    st.info("🎉 Alle Chunks annotiert!")
                
                st.rerun()
            else:
                st.error("Bitte wähle eine Frame-Kategorie!")
    
    with col2:
        if st.button("⏭️ Nächster"):
            if st.session_state.current_chunk_index < len(st.session_state.chunks) - 1:
                st.session_state.current_chunk_index += 1
                st.rerun()
            else:
                st.info("Letzter Chunk erreicht!")
    
    with col3:
        if st.button("⏮️ Vorheriger"):
            if st.session_state.current_chunk_index > 0:
                st.session_state.current_chunk_index -= 1
                st.rerun()
            else:
                st.info("Erster Chunk erreicht!")
    
    # Navigation
    st.subheader("🧭 Navigation")
    
    # Chunk-Index
    new_index = st.number_input(
        "Gehe zu Chunk:",
        min_value=1,
        max_value=len(st.session_state.chunks),
        value=st.session_state.current_chunk_index + 1
    )
    
    if st.button("Gehe zu Chunk"):
        st.session_state.current_chunk_index = new_index - 1
        st.rerun()

def get_classified_chunks() -> List[Dict[str, Any]]:
    """Lädt alle klassifizierten Chunks aus der Datenbank"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                chunk_id,
                speech_id,
                speaker_name,
                speaker_party,
                debate_title,
                debate_date,
                chunk_text,
                frame_label,
                brexit_position,
                assigned_user,
                annotation_notes,
                word_count,
                char_count,
                pre_brexit,
                created_at,
                updated_at
            FROM chunks
            WHERE frame_label IS NOT NULL 
            AND frame_label != ''
            ORDER BY updated_at DESC
        """)
        
        chunks = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Konvertiere zu Dictionary-Liste
        chunk_list = []
        for chunk in chunks:
            chunk_dict = dict(chunk)
            chunk_list.append(chunk_dict)
        
        return chunk_list
        
    except Exception as e:
        st.error(f"Fehler beim Laden der klassifizierten Chunks: {e}")
        if conn:
            conn.close()
        return []

def get_annotation_conflicts() -> List[Dict[str, Any]]:
    """Erkennt echte Annotation-Konflikte zwischen verschiedenen Usern"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Suche nach Chunks mit unterschiedlichen Annotationen von verschiedenen Usern
        cursor.execute("""
            WITH user_annotations AS (
                SELECT 
                    ah.chunk_id,
                    ah.user_name,
                    ah.frame_label,
                    ah.brexit_position,
                    ah.annotation_notes,
                    ah.created_at,
                    c.speech_id,
                    c.speaker_name,
                    c.speaker_party,
                    c.debate_title,
                    c.debate_date,
                    c.chunk_text
                FROM annotation_history ah
                JOIN chunks c ON ah.chunk_id = c.chunk_id
                WHERE ah.frame_label IS NOT NULL AND ah.frame_label != ''
            ),
            conflicts AS (
                SELECT 
                    ua1.chunk_id,
                    ua1.speech_id,
                    ua1.speaker_name,
                    ua1.speaker_party,
                    ua1.debate_title,
                    ua1.debate_date,
                    ua1.chunk_text,
                    ua1.user_name as user1,
                    ua1.frame_label as frame1,
                    ua1.brexit_position as brexit1,
                    ua1.annotation_notes as notes1,
                    ua1.created_at as created1,
                    ua2.user_name as user2,
                    ua2.frame_label as frame2,
                    ua2.brexit_position as brexit2,
                    ua2.annotation_notes as notes2,
                    ua2.created_at as created2
                FROM user_annotations ua1
                JOIN user_annotations ua2 ON ua1.chunk_id = ua2.chunk_id 
                    AND ua1.user_name < ua2.user_name
                WHERE ua1.frame_label != ua2.frame_label
            )
            SELECT * FROM conflicts
            ORDER BY created2 DESC
        """)
        
        conflicts = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Konvertiere zu Dictionary-Liste
        conflict_list = []
        for conflict in conflicts:
            conflict_dict = dict(conflict)
            conflict_list.append(conflict_dict)
        
        return conflict_list
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Konflikte: {e}")
        if conn:
            conn.close()
        return []

def resolve_conflict(chunk_id: str, final_frame: str, final_brexit: str, final_notes: str, resolved_by: str):
    """Löst einen Annotation-Konflikt durch finale Entscheidung"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Aktualisiere die Annotation mit der finalen Entscheidung
        update_sql = """
        UPDATE chunks 
        SET frame_label = %s, brexit_position = %s, annotation_notes = %s, 
            assigned_user = %s, updated_at = CURRENT_TIMESTAMP
        WHERE chunk_id = %s
        """
        
        cursor.execute(update_sql, (final_frame, final_brexit, final_notes, resolved_by, chunk_id))
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        st.error(f"Fehler beim Lösen des Konflikts: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def show_conflict_resolution():
    """Zeigt Interface zur Konfliktlösung"""
    st.subheader("⚔️ Annotation-Konflikte")
    st.markdown("Übersicht über echte Konflikte zwischen verschiedenen Usern und deren Lösung")
    
    # Lade echte Konflikte
    conflicts = get_annotation_conflicts()
    
    if not conflicts:
        st.info("🎉 Keine Konflikte gefunden! Alle Annotationen sind konsistent.")
        return
    
    st.write(f"**Gefundene Konflikte:** {len(conflicts)}")
    
    # Filter-Optionen
    col1, col2, col3 = st.columns(3)
    
    with col1:
        frame_filter = st.selectbox(
            "Frame-Filter:",
            options=["Alle"] + FRAME_CATEGORIES,
            key="conflict_frame_filter"
        )
    
    with col2:
        user_filter = st.selectbox(
            "User-Filter:",
            options=["Alle"] + list(set([c['user1'] for c in conflicts] + [c['user2'] for c in conflicts])),
            key="conflict_user_filter"
        )
    
    with col3:
        brexit_filter = st.selectbox(
            "Brexit-Position Filter:",
            options=["Alle"] + BREXIT_POSITION_CATEGORIES,
            key="conflict_brexit_filter"
        )
    
    # Filtere Konflikte
    filtered_conflicts = conflicts
    
    if frame_filter != "Alle":
        filtered_conflicts = [c for c in filtered_conflicts if c['frame1'] == frame_filter or c['frame2'] == frame_filter]
    
    if user_filter != "Alle":
        filtered_conflicts = [c for c in filtered_conflicts if c['user1'] == user_filter or c['user2'] == user_filter]
    
    if brexit_filter != "Alle":
        filtered_conflicts = [c for c in filtered_conflicts if c['brexit1'] == brexit_filter or c['brexit2'] == brexit_filter]
    
    st.write(f"**Gefilterte Konflikte:** {len(filtered_conflicts)}")
    
    if not filtered_conflicts:
        st.info("Keine Konflikte entsprechen den Filterkriterien.")
        return
    
    # Zeige Konflikte
    for i, conflict in enumerate(filtered_conflicts):
        with st.expander(f"Konflikt {i+1}: {conflict['chunk_id'][:20]}... - {conflict['frame1']} vs {conflict['frame2']}"):
            
            # Chunk-Informationen
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**Speaker:** {conflict['speaker_name']} ({conflict['speaker_party']})")
            with col2:
                st.write(f"**Datum:** {conflict['debate_date']}")
            with col3:
                st.write(f"**Chunk-ID:** {conflict['chunk_id']}")
            
            # Chunk-Text
            st.subheader("📄 Chunk-Text")
            st.text_area("", conflict['chunk_text'], height=150, disabled=True, key=f"text_{conflict['chunk_id']}")
            
            # Beide Annotationen anzeigen
            st.subheader("⚔️ Konfliktierende Annotationen")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 👤 **User 1: " + conflict['user1'] + "**")
                st.write(f"**Frame:** {conflict['frame1']}")
                st.write(f"**Brexit-Position:** {conflict['brexit1'] or 'Nicht gesetzt'}")
                st.write(f"**Notizen:** {conflict['notes1'] or 'Keine Notizen'}")
                st.write(f"**Datum:** {conflict['created1']}")
            
            with col2:
                st.markdown("### 👤 **User 2: " + conflict['user2'] + "**")
                st.write(f"**Frame:** {conflict['frame2']}")
                st.write(f"**Brexit-Position:** {conflict['brexit2'] or 'Nicht gesetzt'}")
                st.write(f"**Notizen:** {conflict['notes2'] or 'Keine Notizen'}")
                st.write(f"**Datum:** {conflict['created2']}")
            
            # Konfliktlösung
            st.subheader("🔧 Konfliktlösung")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Finale Annotation:**")
                new_frame = st.selectbox(
                    "Frame-Kategorie:",
                    options=FRAME_CATEGORIES,
                    index=FRAME_CATEGORIES.index(conflict['frame1']) if conflict['frame1'] in FRAME_CATEGORIES else 0,
                    key=f"new_frame_{conflict['chunk_id']}"
                )
                
                new_brexit = st.selectbox(
                    "Brexit-Position:",
                    options=BREXIT_POSITION_CATEGORIES,
                    index=BREXIT_POSITION_CATEGORIES.index(conflict['brexit1']) if conflict['brexit1'] in BREXIT_POSITION_CATEGORIES else 0,
                    key=f"new_brexit_{conflict['chunk_id']}"
                )
            
            with col2:
                new_notes = st.text_area(
                    "Notizen:",
                    value=f"Konflikt gelöst zwischen {conflict['user1']} und {conflict['user2']}",
                    key=f"new_notes_{conflict['chunk_id']}"
                )
                
                resolved_by = st.text_input(
                    "Gelöst von:",
                    value=st.session_state.user_name or "",
                    key=f"resolved_by_{conflict['chunk_id']}"
                )
            
            # Schnellauswahl-Buttons für häufige Entscheidungen
            st.subheader("⚡ Schnellauswahl")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button(f"✅ {conflict['frame1']}", key=f"quick1_{conflict['chunk_id']}"):
                    st.session_state[f"new_frame_{conflict['chunk_id']}"] = conflict['frame1']
                    st.session_state[f"new_brexit_{conflict['chunk_id']}"] = conflict['brexit1']
                    st.rerun()
            
            with col2:
                if st.button(f"✅ {conflict['frame2']}", key=f"quick2_{conflict['chunk_id']}"):
                    st.session_state[f"new_frame_{conflict['chunk_id']}"] = conflict['frame2']
                    st.session_state[f"new_brexit_{conflict['chunk_id']}"] = conflict['brexit2']
                    st.rerun()
            
            with col3:
                if st.button("🔄 Aktualisieren", key=f"refresh_{conflict['chunk_id']}"):
                    st.rerun()
            
            with col4:
                if st.button("🗑️ Beide entfernen", key=f"delete_{conflict['chunk_id']}"):
                    if st.session_state.user_name:
                        success = resolve_conflict(
                            conflict['chunk_id'], 
                            None, 
                            None, 
                            f"Beide Annotationen entfernt von {st.session_state.user_name}", 
                            st.session_state.user_name
                        )
                        if success:
                            st.success("✅ Beide Annotationen entfernt!")
                            st.rerun()
                        else:
                            st.error("❌ Fehler beim Entfernen der Annotationen!")
                    else:
                        st.error("Bitte gib deinen Namen ein!")
            
            # Finale Lösung
            st.subheader("🎯 Finale Lösung")
            if st.button("✅ Konflikt endgültig lösen", key=f"resolve_{conflict['chunk_id']}", type="primary"):
                if new_frame and resolved_by:
                    success = resolve_conflict(
                        conflict['chunk_id'], 
                        new_frame, 
                        new_brexit, 
                        new_notes, 
                        resolved_by
                    )
                    if success:
                        st.success("✅ Konflikt erfolgreich gelöst!")
                        st.rerun()
                    else:
                        st.error("❌ Fehler beim Lösen des Konflikts!")
                else:
                    st.error("Bitte fülle alle erforderlichen Felder aus!")

def show_classified_chunks():
    """Zeigt alle klassifizierten Chunks in einer filterbaren Tabelle"""
    st.subheader("📋 Klassifizierte Chunks")
    st.markdown("Übersicht über alle bereits klassifizierten Chunks mit Filter- und Suchoptionen")
    
    # Lade klassifizierte Chunks
    chunks = get_classified_chunks()
    
    if not chunks:
        st.info("Keine klassifizierten Chunks gefunden.")
        return
    
    st.write(f"**Gesamt klassifizierte Chunks:** {len(chunks)}")
    
    # Erstelle DataFrame für bessere Darstellung (alle Chunks)
    display_data = []
    for chunk in chunks:
        display_data.append({
            'Chunk-ID': chunk['chunk_id'][:20] + '...' if len(chunk['chunk_id']) > 20 else chunk['chunk_id'],
            'Speaker': chunk['speaker_name'],
            'Party': chunk['speaker_party'],
            'Frame': chunk['frame_label'],
            'Brexit-Pos': chunk['brexit_position'] or 'N/A',
            'User': chunk['assigned_user'],
            'Wörter': chunk['word_count'],
            'Datum': chunk['debate_date'],
            'Pre-Brexit': 'Ja' if chunk['pre_brexit'] else 'Nein',
            'Notizen': chunk['annotation_notes'][:50] + '...' if chunk['annotation_notes'] and len(chunk['annotation_notes']) > 50 else chunk['annotation_notes'] or '',
            'Aktualisiert': chunk['updated_at'].strftime('%Y-%m-%d %H:%M') if chunk['updated_at'] else 'N/A'
        })
    
    df = pd.DataFrame(display_data)
    
    # Zeige Tabelle (alle Chunks)
    st.subheader("📊 Alle klassifizierten Chunks")
    
    # Sortierbare Tabelle
    st.dataframe(
        df,
        use_container_width=True,
        height=600
    )
    
    # Filter-Optionen (optional)
    with st.expander("🔍 Filter & Suche (optional)"):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            frame_filter = st.selectbox(
                "Frame-Kategorie:",
                options=["Alle"] + FRAME_CATEGORIES,
                key="classified_frame_filter"
            )
        
        with col2:
            user_filter = st.selectbox(
                "User:",
                options=["Alle"] + list(set([c['assigned_user'] for c in chunks if c['assigned_user']])),
                key="classified_user_filter"
            )
        
        with col3:
            brexit_filter = st.selectbox(
                "Brexit-Position:",
                options=["Alle"] + BREXIT_POSITION_CATEGORIES,
                key="classified_brexit_filter"
            )
        
        with col4:
            speaker_filter = st.selectbox(
                "Speaker:",
                options=["Alle"] + list(set([c['speaker_name'] for c in chunks if c['speaker_name']])),
                key="classified_speaker_filter"
            )
        
        # Zusätzliche Filter
        col1, col2, col3 = st.columns(3)
        
        with col1:
            pre_brexit_filter = st.selectbox(
                "Brexit-Timing:",
                options=["Alle", "Pre-Brexit", "Post-Brexit"],
                key="classified_timing_filter"
            )
        
        with col2:
            search_text = st.text_input(
                "Text-Suche:",
                placeholder="Suche in Chunk-Text...",
                key="classified_text_search"
            )
        
        with col3:
            date_from = st.date_input(
                "Von Datum:",
                key="classified_date_from"
            )
        
        # Filtere Chunks
        filtered_chunks = chunks
        
        if frame_filter != "Alle":
            filtered_chunks = [c for c in filtered_chunks if c['frame_label'] == frame_filter]
        
        if user_filter != "Alle":
            filtered_chunks = [c for c in filtered_chunks if c['assigned_user'] == user_filter]
        
        if brexit_filter != "Alle":
            filtered_chunks = [c for c in filtered_chunks if c['brexit_position'] == brexit_filter]
        
        if speaker_filter != "Alle":
            filtered_chunks = [c for c in filtered_chunks if c['speaker_name'] == speaker_filter]
        
        if pre_brexit_filter == "Pre-Brexit":
            filtered_chunks = [c for c in filtered_chunks if c['pre_brexit'] == True]
        elif pre_brexit_filter == "Post-Brexit":
            filtered_chunks = [c for c in filtered_chunks if c['pre_brexit'] == False]
        
        if search_text:
            filtered_chunks = [c for c in filtered_chunks if search_text.lower() in c['chunk_text'].lower()]
        
        if date_from:
            filtered_chunks = [c for c in filtered_chunks if c['debate_date'] >= date_from]
        
        st.write(f"**Gefilterte Chunks:** {len(filtered_chunks)}")
        
        if len(filtered_chunks) != len(chunks):
            # Zeige gefilterte Tabelle
            filtered_display_data = []
            for chunk in filtered_chunks:
                filtered_display_data.append({
                    'Chunk-ID': chunk['chunk_id'][:20] + '...' if len(chunk['chunk_id']) > 20 else chunk['chunk_id'],
                    'Speaker': chunk['speaker_name'],
                    'Party': chunk['speaker_party'],
                    'Frame': chunk['frame_label'],
                    'Brexit-Pos': chunk['brexit_position'] or 'N/A',
                    'User': chunk['assigned_user'],
                    'Wörter': chunk['word_count'],
                    'Datum': chunk['debate_date'],
                    'Pre-Brexit': 'Ja' if chunk['pre_brexit'] else 'Nein',
                    'Notizen': chunk['annotation_notes'][:50] + '...' if chunk['annotation_notes'] and len(chunk['annotation_notes']) > 50 else chunk['annotation_notes'] or '',
                    'Aktualisiert': chunk['updated_at'].strftime('%Y-%m-%d %H:%M') if chunk['updated_at'] else 'N/A'
                })
            
            filtered_df = pd.DataFrame(filtered_display_data)
            st.dataframe(filtered_df, use_container_width=True, height=400)
    
    # Aktualisieren-Button
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Aktualisieren"):
            st.rerun()
    
    # Detaillierte Ansicht für ausgewählte Chunks
    st.subheader("🔍 Detaillierte Ansicht")
    
    selected_chunk_id = st.selectbox(
        "Wähle einen Chunk für Details:",
        options=[c['chunk_id'] for c in chunks],
        format_func=lambda x: f"{x[:20]}... - {next(c['frame_label'] for c in chunks if c['chunk_id'] == x)}",
        key="detailed_chunk_selector"
    )
    
    if selected_chunk_id:
        selected_chunk = next(c for c in chunks if c['chunk_id'] == selected_chunk_id)
        
        st.write("**Chunk-Details:**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write(f"**Chunk-ID:** {selected_chunk['chunk_id']}")
            st.write(f"**Speaker:** {selected_chunk['speaker_name']} ({selected_chunk['speaker_party']})")
            st.write(f"**Debatte:** {selected_chunk['debate_title']}")
            st.write(f"**Datum:** {selected_chunk['debate_date']}")
            st.write(f"**Pre-Brexit:** {'Ja' if selected_chunk['pre_brexit'] else 'Nein'}")
        
        with col2:
            st.write(f"**Frame:** {selected_chunk['frame_label']}")
            st.write(f"**Brexit-Position:** {selected_chunk['brexit_position'] or 'Nicht gesetzt'}")
            st.write(f"**User:** {selected_chunk['assigned_user']}")
            st.write(f"**Wörter:** {selected_chunk['word_count']}")
            st.write(f"**Zeichen:** {selected_chunk['char_count']}")
        
        st.write("**Chunk-Text:**")
        st.text_area("", selected_chunk['chunk_text'], height=200, disabled=True)
        
        if selected_chunk['annotation_notes']:
            st.write("**Notizen:**")
            st.write(selected_chunk['annotation_notes'])

def show_admin_view():
    """Zeigt Admin-Ansicht"""
    st.subheader("👥 Admin-Ansicht")
    
    conn = get_db_connection()
    if not conn:
        st.error("Keine Datenbankverbindung!")
        return
    
    try:
        cursor = conn.cursor()
        
        # Alle Zuweisungen
        cursor.execute("""
            SELECT assigned_user, COUNT(*) as total_chunks,
                   COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks
            FROM chunks 
            WHERE assigned_user IS NOT NULL AND assigned_user != ''
            GROUP BY assigned_user
            ORDER BY assigned_user
        """)
        assignments = cursor.fetchall()
        
        if assignments:
            st.write("**Chunk-Zuweisungen:**")
            
            # Erstelle DataFrame für bessere Darstellung
            admin_data = []
            for user, total, annotated in assignments:
                admin_data.append({
                    'User': user,
                    'Zugewiesene Chunks': total,
                    'Annotierte Chunks': annotated,
                    'Fortschritt': f"{annotated}/{total}",
                    'Prozent': f"{(annotated/total*100):.1f}%" if total > 0 else "0%"
                })
            
            df = pd.DataFrame(admin_data)
            st.dataframe(df, use_container_width=True)
        
        # Unzugewiesene Chunks
        cursor.execute("""
            SELECT COUNT(*) FROM chunks 
            WHERE assigned_user IS NULL OR assigned_user = ''
        """)
        unassigned = cursor.fetchone()[0]
        
        st.metric("Unzugewiesene Chunks", f"{unassigned:,}")
        
        # Gesamt-Statistiken
        cursor.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks
            FROM chunks
        """)
        total_stats = cursor.fetchone()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Gesamt Chunks", f"{total_stats[0]:,}")
        with col2:
            st.metric("Annotierte Chunks", f"{total_stats[1]:,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Admin-Daten: {e}")
        if conn:
            conn.close()

def main():
    st.set_page_config(
        page_title="Frame Classification - Railway",
        page_icon="🏷️",
        layout="wide"
    )
    
    st.title("🏷️ Frame Classification - Railway")
    st.markdown("**Multi-User Annotation von Brexit-Debatten Chunks auf Railway**")
    
    # Debug-Informationen (nur in Development)
    if os.getenv('RAILWAY_ENVIRONMENT') != 'production':
        with st.expander("🔧 Debug-Informationen"):
            st.write(f"DATABASE_URL gesetzt: {'Ja' if os.getenv('DATABASE_URL') else 'Nein'}")
            if os.getenv('DATABASE_URL'):
                # Zeige nur den Anfang der URL für Sicherheit
                db_url = os.getenv('DATABASE_URL')
                st.write(f"DATABASE_URL: {db_url[:20]}...")
            else:
                st.write("Verwende lokale Konfiguration")
    
    # Initialisiere Session State
    init_session_state()
    
    # Erstelle Tabellen falls nötig
    create_tables_if_not_exist()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Konfiguration")
        
        # User-Name
        user_name = st.text_input(
            "👤 Dein Name:",
            value=st.session_state.user_name,
            help="Wird für die Zuweisung von Chunks verwendet"
        )
        if user_name != st.session_state.user_name:
            st.session_state.user_name = user_name
            st.rerun()
        
        # Chunk-Limit
        chunk_limit = st.number_input(
            "📊 Chunk-Limit:",
            min_value=10,
            max_value=1000,
            value=100,
            help="Anzahl der Chunks zum Laden"
        )
        
        # Chunk-Typ Auswahl
        chunk_type = st.radio(
            "📝 Chunk-Typ:",
            options=["Nur unklassifizierte", "Alle Chunks"],
            help="Wähle ob nur unklassifizierte Chunks oder alle Chunks geladen werden sollen"
        )
        only_unannotated = chunk_type == "Nur unklassifizierte"
        
        # Lade Chunks
        if st.button("🔄 Chunks laden"):
            if not st.session_state.user_name:
                st.error("Bitte gib zuerst deinen Namen ein!")
            else:
                with st.spinner("Lade Chunks aus PostgreSQL..."):
                    st.session_state.chunks = load_database_chunks(st.session_state.user_name, chunk_limit, only_unannotated)
                    st.session_state.current_chunk_index = 0
                
                chunk_type_text = "unklassifizierte" if only_unannotated else "alle"
                st.success(f"✓ {len(st.session_state.chunks)} {chunk_type_text} Chunks für {st.session_state.user_name} geladen!")
        
        st.divider()
        
        # Statistiken
        if st.button("📊 Statistiken aktualisieren"):
            st.rerun()
    
    # Hauptbereich
    if not st.session_state.chunks:
        st.info("👆 Lade zuerst Chunks aus der Datenbank!")
        st.info("💡 **Tipp:** Wähle 'Nur unklassifizierte' um nur noch zu klassifizierende Chunks zu laden.")
        return
    
    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📝 Annotation", "📊 Statistiken", "👥 Admin", "⚔️ Konflikte", "📋 Klassifiziert"])
    
    with tab1:
        show_chunk_annotation()
    
    with tab2:
        show_statistics()
    
    with tab3:
        show_admin_view()
    
    with tab4:
        show_conflict_resolution()
    
    with tab5:
        show_classified_chunks()

if __name__ == "__main__":
    main()
