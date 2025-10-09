#!/usr/bin/env python3
"""
Einfaches Datenbank-Chunking ohne Lock-Probleme
Erstellt eine neue Datenbank mit Chunks-Tabelle
"""

import duckdb
import json
import re
import argparse
from pathlib import Path
from typing import List, Dict, Any
import spacy
from spacy.lang.de import German
import nltk
from nltk.tokenize import sent_tokenize
import shutil

# Konfiguration
INPUT_DB = "../../data/processed/debates_brexit_filtered.duckdb"
OUTPUT_DB = "../../data/processed/debates_brexit_chunked.duckdb"

class SimpleDatabaseChunker:
    def __init__(self):
        self.nlp = None
        
        try:
            self.nlp = spacy.load("de_core_news_sm")
            print("✓ spaCy de_core_news_sm geladen")
        except OSError:
            print("⚠️ spaCy de_core_news_sm nicht gefunden, verwende Basic German")
            self.nlp = German()
    
    def clean_text(self, text: str) -> str:
        """Bereinigt Text für bessere Verarbeitung"""
        if not text:
            return ""
        
        # Entferne HTML-Tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Entferne übermäßige Leerzeichen
        text = re.sub(r'\s+', ' ', text)
        
        # Entferne Zeilenumbrüche
        text = text.replace('\n', ' ').replace('\r', ' ')
        
        return text.strip()
    
    def split_into_sentences(self, text: str) -> List[str]:
        """Teilt Text in Sätze auf"""
        if not text:
            return []
        
        # Versuche spaCy zuerst
        if self.nlp:
            try:
                doc = self.nlp(text)
                return [sent.text.strip() for sent in doc.sents if sent.text.strip()]
            except:
                pass
        
        # Fallback: NLTK
        try:
            return sent_tokenize(text)
        except:
            pass
        
        # Fallback: Einfache Regex
        sentences = re.split(r'[.!?]+', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def chunk_by_semantic_boundaries(self, text: str, max_chars: int = 800) -> List[str]:
        """Chunking nach semantischen Grenzen"""
        if not text:
            return []
        
        sentences = self.split_into_sentences(text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            if len(current_chunk + " " + sentence) <= max_chars:
                current_chunk += " " + sentence if current_chunk else sentence
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def assign_chunks_to_annotators(self, conn, total_chunks: int):
        """Weist 1.000 Chunks zufällig den Annotatoren zu - JEDER CHUNK wird von 2 USERN annotiert"""
        print("Weise Chunks den Annotatoren zu (Doppel-Annotation)...")
        
        # Annotatoren
        annotators = ["Max", "Julian", "Lina", "Julius", "Rike"]
        
        # Feste Anzahl: 1.000 Chunks für Annotation
        annotation_count = 1000
        print(f"Wähle {annotation_count} Chunks für Annotation aus")
        print(f"Jeder Chunk wird von 2 Usern annotiert = {annotation_count * 2} Annotationen")
        
        # Zufällige Auswahl der Chunk-IDs
        import random
        random.seed(42)  # Für reproduzierbare Ergebnisse
        
        # Hole alle Chunk-IDs
        all_chunk_ids = conn.execute("SELECT chunk_id FROM chunks ORDER BY chunk_id").fetchall()
        all_chunk_ids = [row[0] for row in all_chunk_ids]
        
        # Prüfe ob genügend Chunks verfügbar sind
        if len(all_chunk_ids) < annotation_count:
            print(f"⚠️ Nur {len(all_chunk_ids)} Chunks verfügbar, aber {annotation_count} benötigt")
            annotation_count = len(all_chunk_ids)
            print(f"Verwende alle verfügbaren {annotation_count} Chunks")
        
        # Zufällige Auswahl
        selected_chunk_ids = random.sample(all_chunk_ids, annotation_count)
        
        # Erstelle Agreement-Tabelle
        self.create_agreement_table(conn)
        
        # Zuweisung: Jeder Chunk an 2 zufällige Annotatoren
        assignment_stats = {annotator: 0 for annotator in annotators}
        agreement_pairs = []
        
        for chunk_id in selected_chunk_ids:
            # Wähle 2 zufällige Annotatoren
            selected_annotators = random.sample(annotators, 2)
            
            # Erste Annotation
            conn.execute(
                "UPDATE chunks SET assigned_user = ? WHERE chunk_id = ?",
                (selected_annotators[0], chunk_id)
            )
            assignment_stats[selected_annotators[0]] += 1
            
            # Zweite Annotation (Duplikat für Agreement)
            conn.execute("""
                INSERT INTO chunks (
                    chunk_id, speech_id, debate_id, speaker_name, speaker_party,
                    debate_title, debate_date, chunk_text, chunk_index, total_chunks,
                    word_count, char_count, chunking_method, assigned_user,
                    frame_label, annotation_confidence, annotation_notes
                )
                SELECT 
                    chunk_id || '_dup', speech_id, debate_id, speaker_name, speaker_party,
                    debate_title, debate_date, chunk_text, chunk_index, total_chunks,
                    word_count, char_count, chunking_method, ?, 
                    frame_label, annotation_confidence, annotation_notes
                FROM chunks 
                WHERE chunk_id = ?
            """, (selected_annotators[1], chunk_id))
            
            assignment_stats[selected_annotators[1]] += 1
            
            # Speichere Agreement-Paar
            agreement_pairs.append({
                'chunk_id': chunk_id,
                'annotator1': selected_annotators[0],
                'annotator2': selected_annotators[1]
            })
        
        # Speichere Agreement-Paare
        for pair in agreement_pairs:
            conn.execute("""
                INSERT INTO agreement_chunks (chunk_id, annotator1, annotator2)
                VALUES (?, ?, ?)
            """, (pair['chunk_id'], pair['annotator1'], pair['annotator2']))
        
        print("✓ Chunk-Zuweisung abgeschlossen (Doppel-Annotation):")
        for annotator, count in assignment_stats.items():
            print(f"  {annotator}: {count} Chunks")
        
        print(f"✓ {annotation_count} Chunks für Annotation zugewiesen")
        print(f"✓ {annotation_count * 2} Annotationen insgesamt")
        print(f"✓ {total_chunks - annotation_count} Chunks bleiben unzugewiesen")
    
    def create_agreement_table(self, conn):
        """Erstellt Agreement-Tabelle für Doppel-Annotation"""
        print("Erstelle Agreement-Tabelle...")
        
        create_agreement_sql = """
        CREATE TABLE IF NOT EXISTS agreement_chunks (
            chunk_id VARCHAR PRIMARY KEY,
            annotator1 VARCHAR NOT NULL,
            annotator2 VARCHAR NOT NULL,
            label1 VARCHAR,
            label2 VARCHAR,
            agreement_score FLOAT,
            agreement_perfect BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        conn.execute(create_agreement_sql)
        
        # Erstelle Indizes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agreement_annotator1 ON agreement_chunks(annotator1)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agreement_annotator2 ON agreement_chunks(annotator2)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agreement_score ON agreement_chunks(agreement_score)")
        
        print("✓ Agreement-Tabelle erstellt")
    
    def create_new_database(self, input_db: str, output_db: str, max_speeches: int = None):
        """Erstellt eine neue Datenbank mit Chunks"""
        print("=" * 70)
        print("EINFACHES DATENBANK-CHUNKING")
        print("=" * 70)
        
        # Lösche Output-DB falls vorhanden
        if Path(output_db).exists():
            Path(output_db).unlink()
        
        # Erstelle neue Datenbank
        conn = duckdb.connect(output_db)
        
        # Kopiere Tabellen aus Input-DB
        print("Kopiere Tabellen aus Input-Datenbank...")
        
        # Öffne Input-DB
        input_conn = duckdb.connect(input_db, read_only=True)
        
        # Hole alle Tabellen
        tables = input_conn.execute("SHOW TABLES").fetchall()
        print(f"Gefundene Tabellen: {[t[0] for t in tables]}")
        
        # Kopiere jede Tabelle
        for table_name in [t[0] for t in tables]:
            print(f"  Kopiere {table_name}...")
            
            # Hole Schema
            schema = input_conn.execute(f"DESCRIBE {table_name}").fetchall()
            create_sql = f"CREATE TABLE {table_name} ("
            columns = []
            for col in schema:
                col_def = f"{col[0]} {col[1]}"
                if col[2] == "NO":
                    col_def += " NOT NULL"
                columns.append(col_def)
            create_sql += ", ".join(columns) + ")"
            
            # Erstelle Tabelle
            conn.execute(create_sql)
            
            # Kopiere Daten
            data = input_conn.execute(f"SELECT * FROM {table_name}").fetchall()
            if data:
                columns = [col[0] for col in schema]
                placeholders = ", ".join(["?" for _ in columns])
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                
                for row in data:
                    conn.execute(insert_sql, row)
                
                print(f"    ✓ {len(data):,} Zeilen kopiert")
        
        input_conn.close()
        
        # Erstelle Chunks-Tabelle
        print("Erstelle Chunks-Tabelle...")
        create_chunks_sql = """
        CREATE TABLE chunks (
            chunk_id VARCHAR PRIMARY KEY,
            speech_id VARCHAR NOT NULL,
            debate_id VARCHAR,
            speaker_name VARCHAR,
            speaker_party VARCHAR,
            debate_title VARCHAR,
            debate_date DATE,
            chunk_text TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            total_chunks INTEGER NOT NULL,
            word_count INTEGER NOT NULL,
            char_count INTEGER NOT NULL,
            chunking_method VARCHAR NOT NULL,
            assigned_user VARCHAR,
            frame_label VARCHAR,
            annotation_confidence INTEGER,
            annotation_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        conn.execute(create_chunks_sql)
        print("✓ Chunks-Tabelle erstellt")
        
        # Verarbeite Reden zu Chunks
        print("Verarbeite Reden zu intelligenten Chunks...")
        
        # Lade Speeches
        if max_speeches:
            query = f"""
            SELECT 
                s.speech_id,
                s.debate_id,
                s.speaker_name,
                s.speaker_office,
                s.speech_text,
                s.time,
                d.major_heading_text,
                d.date
            FROM speeches s
            LEFT JOIN debates d ON s.debate_id = d.debate_id
            ORDER BY s.speech_id
            LIMIT {max_speeches}
            """
            print(f"Begrenzt auf {max_speeches} Reden für Test")
        else:
            query = """
            SELECT 
                s.speech_id,
                s.debate_id,
                s.speaker_name,
                s.speaker_office,
                s.speech_text,
                s.time,
                d.major_heading_text,
                d.date
            FROM speeches s
            LEFT JOIN debates d ON s.debate_id = d.debate_id
            ORDER BY s.speech_id
            """
        
        speeches = conn.execute(query).fetchall()
        print(f"✓ {len(speeches)} Reden geladen")
        
        # Verarbeite zu Chunks
        all_chunks = []
        chunk_id = 0
        
        for speech in speeches:
            speech_id = speech[0]
            debate_id = speech[1]
            speaker_name = speech[2]
            speaker_party = speech[3]
            speech_text = speech[4]
            time = speech[5]
            debate_title = speech[6]
            debate_date = speech[7]
            
            if not speech_text:
                continue
            
            # Bereinige Text
            clean_text = self.clean_text(speech_text)
            if not clean_text:
                continue
            
            # Erstelle Chunks
            chunks = self.chunk_by_semantic_boundaries(clean_text)
            
            for i, chunk_text in enumerate(chunks):
                if not chunk_text.strip():
                    continue
                
                chunk_data = {
                    'chunk_id': f"chunk_{chunk_id:06d}",
                    'speech_id': speech_id,
                    'debate_id': debate_id,
                    'speaker_name': speaker_name,
                    'speaker_party': speaker_party,
                    'debate_title': debate_title,
                    'debate_date': debate_date,
                    'chunk_text': chunk_text,
                    'chunk_index': i,
                    'total_chunks': len(chunks),
                    'word_count': len(chunk_text.split()),
                    'char_count': len(chunk_text),
                    'chunking_method': 'semantic',
                    'assigned_user': None,
                    'frame_label': None,
                    'annotation_confidence': None,
                    'annotation_notes': None
                }
                
                all_chunks.append(chunk_data)
                chunk_id += 1
        
        print(f"✓ {len(all_chunks)} intelligente Chunks erstellt")
        
        # Füge Chunks in Datenbank ein
        print("Füge Chunks in Datenbank ein...")
        
        insert_sql = """
        INSERT INTO chunks (
            chunk_id, speech_id, debate_id, speaker_name, speaker_party,
            debate_title, debate_date, chunk_text, chunk_index, total_chunks,
            word_count, char_count, chunking_method, assigned_user,
            frame_label, annotation_confidence, annotation_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for chunk in all_chunks:
            conn.execute(insert_sql, (
                chunk['chunk_id'],
                chunk['speech_id'],
                chunk['debate_id'],
                chunk['speaker_name'],
                chunk['speaker_party'],
                chunk['debate_title'],
                chunk['debate_date'],
                chunk['chunk_text'],
                chunk['chunk_index'],
                chunk['total_chunks'],
                chunk['word_count'],
                chunk['char_count'],
                chunk['chunking_method'],
                chunk['assigned_user'],
                chunk['frame_label'],
                chunk['annotation_confidence'],
                chunk['annotation_notes']
            ))
        
        print(f"✓ {len(all_chunks)} Chunks in Datenbank eingefügt")
        
        # Erstelle Indizes
        print("Erstelle Indizes...")
        indexes = [
            "CREATE INDEX idx_chunks_speech_id ON chunks(speech_id)",
            "CREATE INDEX idx_chunks_debate_id ON chunks(debate_id)",
            "CREATE INDEX idx_chunks_speaker ON chunks(speaker_name)",
            "CREATE INDEX idx_chunks_assigned_user ON chunks(assigned_user)",
            "CREATE INDEX idx_chunks_frame_label ON chunks(frame_label)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        print("✓ Indizes erstellt")
        
        # Zufällige Auswahl und Zuweisung
        self.assign_chunks_to_annotators(conn, len(all_chunks))
        
        # Statistiken
        print("\n" + "=" * 70)
        print("STATISTIKEN")
        print("=" * 70)
        
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                AVG(word_count) as avg_words,
                AVG(char_count) as avg_chars,
                MIN(word_count) as min_words,
                MAX(word_count) as max_words
            FROM chunks
        """).fetchone()
        
        print(f"Gesamt Chunks:        {stats[0]:,}")
        print(f"Durchschnitt Wörter:   {stats[1]:.1f}")
        print(f"Durchschnitt Zeichen:  {stats[2]:.1f}")
        print(f"Wort-Range:           {stats[3]}-{stats[4]}")
        
        # Zuweisungs-Statistiken
        assignment_stats = conn.execute("""
            SELECT assigned_user, COUNT(*) as count
            FROM chunks 
            WHERE assigned_user IS NOT NULL AND assigned_user != ''
            GROUP BY assigned_user
            ORDER BY assigned_user
        """).fetchall()
        
        print(f"\nChunk-Zuweisungen:")
        for user, count in assignment_stats:
            print(f"  {user:10}: {count:6,} Chunks")
        
        # Unzugewiesene Chunks
        unassigned = conn.execute("""
            SELECT COUNT(*) FROM chunks 
            WHERE assigned_user IS NULL OR assigned_user = ''
        """).fetchone()[0]
        
        print(f"  Unzugewiesen: {unassigned:6,} Chunks")
        
        conn.close()
        print(f"\n✓ Chunking abgeschlossen!")
        print(f"✓ Neue Datenbank: {output_db}")

def main():
    parser = argparse.ArgumentParser(description='Einfaches Datenbank-Chunking')
    parser.add_argument('--input-db', default=INPUT_DB, help='Pfad zur Input-Datenbank')
    parser.add_argument('--output-db', default=OUTPUT_DB, help='Pfad zur Output-Datenbank')
    parser.add_argument('--max-speeches', type=int, help='Maximale Anzahl Reden (für Tests)')
    
    args = parser.parse_args()
    
    # Prüfe Input-Datenbank
    if not Path(args.input_db).exists():
        print(f"✗ Input-Datenbank {args.input_db} nicht gefunden!")
        return
    
    # Erstelle Chunker
    chunker = SimpleDatabaseChunker()
    
    # Führe Chunking aus
    chunker.create_new_database(args.input_db, args.output_db, args.max_speeches)

if __name__ == "__main__":
    main()
