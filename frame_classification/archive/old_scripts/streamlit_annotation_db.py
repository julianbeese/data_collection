#!/usr/bin/env python3
"""
Streamlit Annotation Interface für Datenbank-basierte Chunks
Nutzt die neue chunks-Tabelle in der DuckDB-Datenbank
"""

import streamlit as st
import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import plotly.express as px
import plotly.graph_objects as go

# Konfiguration
DATABASE_PATH = "../data/processed/debates_brexit_chunked_agreement.duckdb"
ANNOTATIONS_FILE = "annotations/annotations_db.json"

# Frame-Kategorien
FRAME_CATEGORIES = [
    "Human Impact",
    "Powerlessness", 
    "Economic",
    "Moral Value",
    "Conflict",
    "Other"
]

# Confidence-Level (vorerst deaktiviert)
# CONFIDENCE_LEVELS = {
#     1: "Sehr unsicher",
#     2: "Unsicher", 
#     3: "Neutral",
#     4: "Sicher",
#     5: "Sehr sicher"
# }

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

def load_database_chunks(user_name: str = None, limit: int = None) -> List[Dict[str, Any]]:
    """Lädt Chunks aus der Datenbank für einen bestimmten User"""
    try:
        conn = duckdb.connect(DATABASE_PATH, read_only=True)
        
        if user_name:
            # Lade nur Chunks für den spezifischen User
            if limit:
                query = f"""
                SELECT * FROM chunks 
                WHERE assigned_user = ?
                ORDER BY chunk_id 
                LIMIT {limit}
                """
                params = (user_name,)
            else:
                query = """
                SELECT * FROM chunks 
                WHERE assigned_user = ?
                ORDER BY chunk_id
                """
                params = (user_name,)
        else:
            # Lade alle unzugewiesenen Chunks (für Admin-View)
            if limit:
                query = f"""
                SELECT * FROM chunks 
                WHERE assigned_user IS NULL OR assigned_user = ''
                ORDER BY chunk_id 
                LIMIT {limit}
                """
                params = ()
            else:
                query = """
                SELECT * FROM chunks 
                WHERE assigned_user IS NULL OR assigned_user = ''
                ORDER BY chunk_id
                """
                params = ()
        
        chunks = conn.execute(query, params).fetchall()
        conn.close()
        
        # Konvertiere zu Dictionary-Liste
        chunk_list = []
        for chunk in chunks:
            chunk_dict = {
                'chunk_id': chunk[0],
                'speech_id': chunk[1],
                'debate_id': chunk[2],
                'speaker_name': chunk[3],
                'speaker_party': chunk[4],
                'debate_title': chunk[5],
                'debate_date': chunk[6],
                'chunk_text': chunk[7],
                'chunk_index': chunk[8],
                'total_chunks': chunk[9],
                'word_count': chunk[10],
                'char_count': chunk[11],
                'chunking_method': chunk[12],
                'assigned_user': chunk[13],
                'frame_label': chunk[14],
                'annotation_confidence': chunk[15],
                'annotation_notes': chunk[16],
                'created_at': chunk[17],
                'updated_at': chunk[18]
            }
            chunk_list.append(chunk_dict)
        
        return chunk_list
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Chunks: {e}")
        return []

def load_annotations() -> Dict[str, Any]:
    """Lädt gespeicherte Annotationen"""
    try:
        if Path(ANNOTATIONS_FILE).exists():
            with open(ANNOTATIONS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        st.warning(f"Fehler beim Laden der Annotationen: {e}")
        return {}

def save_annotations(annotations: Dict[str, Any]):
    """Speichert Annotationen"""
    try:
        Path(ANNOTATIONS_FILE).parent.mkdir(exist_ok=True)
        with open(ANNOTATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(annotations, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"Fehler beim Speichern: {e}")

def update_database_annotation(chunk_id: str, frame_label: str, confidence: int, notes: str, user_name: str):
    """Aktualisiert Annotation in der Datenbank und berechnet Agreement"""
    try:
        conn = duckdb.connect(DATABASE_PATH)
        
        update_sql = """
        UPDATE chunks 
        SET frame_label = ?, annotation_confidence = ?, annotation_notes = ?, 
            assigned_user = ?, updated_at = CURRENT_TIMESTAMP
        WHERE chunk_id = ?
        """
        
        conn.execute(update_sql, (frame_label, confidence, notes, user_name, chunk_id))
        
        # Berechne Agreement für diesen Chunk
        calculate_agreement_for_chunk(conn, chunk_id)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Fehler beim Aktualisieren der Datenbank: {e}")

def calculate_agreement_for_chunk(conn, chunk_id: str):
    """Berechnet Agreement für einen spezifischen Chunk"""
    try:
        # Prüfe ob Agreement-Tabelle existiert
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'agreement_chunks' not in table_names:
            return
        
        # Hole Agreement-Paar für diesen Chunk
        agreement_pair = conn.execute("""
            SELECT annotator1, annotator2 
            FROM agreement_chunks 
            WHERE chunk_id = ?
        """, (chunk_id,)).fetchone()
        
        if not agreement_pair:
            return
        
        annotator1, annotator2 = agreement_pair
        
        # Hole Annotationen beider Annotatoren
        annotations = conn.execute("""
            SELECT assigned_user, frame_label 
            FROM chunks 
            WHERE (chunk_id = ? OR chunk_id = ? || '_dup') 
            AND assigned_user IN (?, ?)
            AND frame_label IS NOT NULL
        """, (chunk_id, chunk_id, annotator1, annotator2)).fetchall()
        
        if len(annotations) == 2:
            # Finde die richtigen Annotationen
            label1, label2 = None, None
            for user, label in annotations:
                if user == annotator1:
                    label1 = label
                elif user == annotator2:
                    label2 = label
            
            if label1 and label2:
                # Berechne Agreement
                agreement_score = 1.0 if label1 == label2 else 0.0
                agreement_perfect = label1 == label2
                
                # Update Agreement-Tabelle
                conn.execute("""
                    UPDATE agreement_chunks 
                    SET label1 = ?, label2 = ?, agreement_score = ?, 
                        agreement_perfect = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE chunk_id = ?
                """, (label1, label2, agreement_score, agreement_perfect, chunk_id))
        
    except Exception as e:
        # Agreement-Berechnung ist optional, nicht kritisch
        pass

def get_statistics() -> Dict[str, Any]:
    """Berechnet Statistiken"""
    try:
        conn = duckdb.connect(DATABASE_PATH, read_only=True)
        
        # Gesamt-Statistiken
        total_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks,
                COUNT(CASE WHEN assigned_user IS NOT NULL AND assigned_user != '' THEN 1 END) as assigned_chunks
            FROM chunks
        """).fetchone()
        
        # Frame-Verteilung
        frame_stats = conn.execute("""
            SELECT frame_label, COUNT(*) as count
            FROM chunks 
            WHERE frame_label IS NOT NULL
            GROUP BY frame_label
            ORDER BY count DESC
        """).fetchall()
        
        # User-Statistiken
        user_stats = conn.execute("""
            SELECT assigned_user, COUNT(*) as count
            FROM chunks 
            WHERE assigned_user IS NOT NULL AND assigned_user != ''
            GROUP BY assigned_user
            ORDER BY count DESC
        """).fetchall()
        
        conn.close()
        
        return {
            'total_chunks': total_stats[0],
            'annotated_chunks': total_stats[1],
            'assigned_chunks': total_stats[2],
            'by_frame': {frame: count for frame, count in frame_stats},
            'by_user': {user: count for user, count in user_stats}
        }
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Statistiken: {e}")
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
    
    # Confidence (vorerst deaktiviert)
    # confidence = st.slider(
    #     "Confidence (1-5):",
    #     min_value=1,
    #     max_value=5,
    #     value=3,
    #     key=f"confidence_{chunk_id}"
    # )
    # st.write(f"**{CONFIDENCE_LEVELS[confidence]}**")
    
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
                # Speichere in Session State
                st.session_state.annotations[chunk_id] = {
                    'chunk_id': chunk_id,
                    'frame_label': frame_label,
                    'confidence': 3,  # Standard-Wert
                    'notes': notes,
                    'user_name': st.session_state.user_name,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Speichere in Datei
                save_annotations(st.session_state.annotations)
                
                # Aktualisiere Datenbank
                update_database_annotation(
                    chunk_id, frame_label, 3, notes, st.session_state.user_name  # Standard confidence = 3
                )
                
                st.success("✅ Annotation gespeichert!")
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

def show_admin_view():
    """Zeigt Admin-Ansicht mit allen Zuweisungen und Agreement"""
    st.subheader("👥 Admin-Ansicht")
    
    try:
        conn = duckdb.connect(DATABASE_PATH, read_only=True)
        
        # Alle Zuweisungen
        assignments = conn.execute("""
            SELECT assigned_user, COUNT(*) as total_chunks,
                   COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks
            FROM chunks 
            WHERE assigned_user IS NOT NULL AND assigned_user != ''
            GROUP BY assigned_user
            ORDER BY assigned_user
        """).fetchall()
        
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
            
            # Fortschritts-Chart
            if len(admin_data) > 1:
                fig = px.bar(
                    df, 
                    x='User', 
                    y='Annotierte Chunks',
                    title="Annotierungs-Fortschritt pro User",
                    color='Annotierte Chunks',
                    color_continuous_scale='viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Agreement-Statistiken
        st.subheader("🤝 Inter-Annotator Agreement")
        
        # Prüfe ob Agreement-Tabelle existiert
        try:
            agreement_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_pairs,
                    COUNT(CASE WHEN label1 IS NOT NULL AND label2 IS NOT NULL THEN 1 END) as completed_pairs,
                    COUNT(CASE WHEN label1 = label2 THEN 1 END) as perfect_matches,
                    AVG(CASE WHEN label1 = label2 THEN 1.0 ELSE 0.0 END) as agreement_rate
                FROM agreement_chunks
            """).fetchone()
            
            if agreement_stats and agreement_stats[0] > 0:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Agreement-Paare", f"{agreement_stats[0]:,}")
                
                with col2:
                    st.metric("Abgeschlossen", f"{agreement_stats[1]:,}")
                
                with col3:
                    st.metric("Perfekte Matches", f"{agreement_stats[2]:,}")
                
                with col4:
                    agreement_pct = agreement_stats[3] * 100 if agreement_stats[3] else 0
                    st.metric("Agreement-Rate", f"{agreement_pct:.1f}%")
                
                # Agreement-Details
                if agreement_stats[1] > 0:
                    st.write("**Agreement-Details:**")
                    
                    # User-Paar Agreement
                    pair_agreements = conn.execute("""
                        SELECT 
                            annotator1, annotator2,
                            COUNT(*) as total_pairs,
                            COUNT(CASE WHEN label1 = label2 THEN 1 END) as matches,
                            AVG(CASE WHEN label1 = label2 THEN 1.0 ELSE 0.0 END) as agreement_rate
                        FROM agreement_chunks 
                        WHERE label1 IS NOT NULL AND label2 IS NOT NULL
                        GROUP BY annotator1, annotator2
                        ORDER BY agreement_rate DESC
                    """).fetchall()
                    
                    if pair_agreements:
                        pair_data = []
                        for ann1, ann2, total, matches, rate in pair_agreements:
                            pair_data.append({
                                'Annotator 1': ann1,
                                'Annotator 2': ann2,
                                'Paare': total,
                                'Matches': matches,
                                'Agreement': f"{rate*100:.1f}%"
                            })
                        
                        pair_df = pd.DataFrame(pair_data)
                        st.dataframe(pair_df, use_container_width=True)
                        
                        # Agreement-Chart
                        if len(pair_data) > 1:
                            fig = px.bar(
                                pair_df,
                                x='Annotator 1',
                                y='Agreement',
                                color='Annotator 2',
                                title="Agreement-Rate zwischen User-Paaren",
                                text='Agreement'
                            )
                            fig.update_traces(texttemplate='%{text}', textposition='outside')
                            st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Noch keine Agreement-Daten verfügbar")
                
        except Exception as e:
            st.warning(f"Agreement-Daten nicht verfügbar: {e}")
        
        # Unzugewiesene Chunks
        unassigned = conn.execute("""
            SELECT COUNT(*) FROM chunks 
            WHERE assigned_user IS NULL OR assigned_user = ''
        """).fetchone()[0]
        
        st.metric("Unzugewiesene Chunks", f"{unassigned:,}")
        
        # Gesamt-Statistiken
        total_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                COUNT(CASE WHEN frame_label IS NOT NULL THEN 1 END) as annotated_chunks
            FROM chunks
        """).fetchone()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Gesamt Chunks", f"{total_stats[0]:,}")
        with col2:
            st.metric("Annotierte Chunks", f"{total_stats[1]:,}")
        
        conn.close()
        
    except Exception as e:
        st.error(f"Fehler beim Laden der Admin-Daten: {e}")

def export_annotations():
    """Exportiert Annotationen"""
    if not st.session_state.annotations:
        st.info("Keine Annotationen zum Exportieren")
        return
    
    try:
        # Erstelle CSV-Export
        export_data = []
        for ann in st.session_state.annotations.values():
            export_data.append({
                'chunk_id': str(ann.get('chunk_id', '')),
                'frame_label': str(ann.get('frame_label', '')),
                'notes': str(ann.get('notes', '')),
                'user_name': str(ann.get('user_name', '')),
                'timestamp': str(ann.get('timestamp', ''))
            })
        
        df = pd.DataFrame(export_data)
        csv = df.to_csv(index=False)
        
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"frame_annotations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Fehler beim CSV-Export: {e}")

def main():
    st.set_page_config(
        page_title="Frame Classification - Database",
        page_icon="🏷️",
        layout="wide"
    )
    
    st.title("🏷️ Frame Classification - Database Interface")
    st.markdown("**Intelligente Annotation von Brexit-Debatten Chunks**")
    
    # Initialisiere Session State
    init_session_state()
    
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
        
        # Lade Chunks
        if st.button("🔄 Chunks laden"):
            if not st.session_state.user_name:
                st.error("Bitte gib zuerst deinen Namen ein!")
            else:
                with st.spinner("Lade Chunks aus Datenbank..."):
                    st.session_state.chunks = load_database_chunks(st.session_state.user_name, chunk_limit)
                    st.session_state.current_chunk_index = 0
                    st.session_state.annotations = load_annotations()
                st.success(f"✓ {len(st.session_state.chunks)} Chunks für {st.session_state.user_name} geladen!")
        
        st.divider()
        
        # Statistiken
        if st.button("📊 Statistiken aktualisieren"):
            st.rerun()
    
    # Hauptbereich
    if not st.session_state.chunks:
        st.info("👆 Lade zuerst Chunks aus der Datenbank!")
        return
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Annotation", "📊 Statistiken", "👥 Admin", "📥 Export"])
    
    with tab1:
        show_chunk_annotation()
    
    with tab2:
        show_statistics()
    
    with tab3:
        show_admin_view()
    
    with tab4:
        st.subheader("📥 Export")
        export_annotations()
        
        # JSON-Export
        if st.button("📄 JSON Export"):
            json_data = json.dumps(st.session_state.annotations, ensure_ascii=False, indent=2)
            st.download_button(
                label="📥 Download JSON",
                data=json_data,
                file_name=f"annotations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

if __name__ == "__main__":
    main()
