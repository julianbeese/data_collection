#!/usr/bin/env python3
"""
Datenbank-basiertes intelligentes Chunking für Frame-Classification
Erstellt eine Kopie der Datenbank und fügt Chunks-Tabelle hinzu
"""

import duckdb
import json
import re
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple
import spacy
from spacy.lang.de import German
import nltk
from nltk.tokenize import sent_tokenize
import shutil
from datetime import datetime

# Konfiguration
INPUT_DB = "../../data/processed/debates_brexit_filtered.duckdb"
OUTPUT_DB = "../../data/processed/debates_brexit_chunked.duckdb"

class DatabaseChunker:
    def __init__(self, method: str = "semantic"):
        self.method = method
        self.nlp = None
        
        if method == "semantic":
            try:
                self.nlp = spacy.load("de_core_news_sm")
                print("✓ spaCy de_core_news_sm geladen")
            except OSError:
                print("⚠️ spaCy de_core_news_sm nicht gefunden, verwende Basic German")
                self.nlp = German()
        
        # NLTK für Fallback
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            print("⚠️ NLTK punkt nicht gefunden, verwende einfache Satz-Tokenisierung")
    
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
        """Chunking nach semantischen Grenzen (Sätze, Absätze)"""
        if not text:
            return []
        
        # Teile in Sätze
        sentences = self.split_into_sentences(text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # Prüfe ob Satz hinzugefügt werden kann
            if len(current_chunk + " " + sentence) <= max_chars:
                current_chunk += " " + sentence if current_chunk else sentence
            else:
                # Speichere aktuellen Chunk
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def create_database_copy(self, input_db: str, output_db: str):
        """Erstellt eine Kopie der Datenbank"""
        print(f"Erstelle Kopie der Datenbank: {output_db}")
        
        # Lösche Output-DB falls vorhanden
        if Path(output_db).exists():
            Path(output_db).unlink()
        
        # Kopiere Datei
        shutil.copy2(input_db, output_db)
        print("✓ Datenbank kopiert")
    
    def create_chunks_table(self, conn):
        """Erstellt die Chunks-Tabelle"""
        print("Erstelle Chunks-Tabelle...")
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS chunks (
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
        
        conn.execute(create_table_sql)
        print("✓ Chunks-Tabelle erstellt")
    
    def process_speeches_to_chunks(self, conn, max_speeches: int = None):
        """Verarbeitet alle Reden zu Chunks"""
        print("Verarbeite Reden zu intelligenten Chunks...")
        
        # Lade alle Speeches (mit optionaler Begrenzung)
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
                    'chunking_method': self.method,
                    'assigned_user': None,  # Wird später zugewiesen
                    'frame_label': None,
                    'annotation_confidence': None,
                    'annotation_notes': None
                }
                
                all_chunks.append(chunk_data)
                chunk_id += 1
        
        print(f"✓ {len(all_chunks)} intelligente Chunks erstellt")
        return all_chunks
    
    def insert_chunks_to_database(self, conn, chunks: List[Dict[str, Any]]):
        """Fügt Chunks in die Datenbank ein"""
        print("Füge Chunks in Datenbank ein...")
        
        insert_sql = """
        INSERT INTO chunks (
            chunk_id, speech_id, debate_id, speaker_name, speaker_party,
            debate_title, debate_date, chunk_text, chunk_index, total_chunks,
            word_count, char_count, chunking_method, assigned_user,
            frame_label, annotation_confidence, annotation_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for chunk in chunks:
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
        
        print(f"✓ {len(chunks)} Chunks in Datenbank eingefügt")
    
    def create_indexes(self, conn):
        """Erstellt Indizes für bessere Performance"""
        print("Erstelle Indizes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_chunks_speech_id ON chunks(speech_id)",
            "CREATE INDEX IF NOT EXISTS idx_chunks_debate_id ON chunks(debate_id)",
            "CREATE INDEX IF NOT EXISTS idx_chunks_speaker ON chunks(speaker_name)",
            "CREATE INDEX IF NOT EXISTS idx_chunks_assigned_user ON chunks(assigned_user)",
            "CREATE INDEX IF NOT EXISTS idx_chunks_frame_label ON chunks(frame_label)",
            "CREATE INDEX IF NOT EXISTS idx_chunks_method ON chunks(chunking_method)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        print("✓ Indizes erstellt")
    
    def run_chunking_pipeline(self, input_db: str, output_db: str, max_speeches: int = None):
        """Führt die komplette Chunking-Pipeline aus"""
        print("=" * 70)
        print("DATENBANK-BASIERTES INTELLIGENTES CHUNKING")
        print("=" * 70)
        
        # 1. Erstelle Datenbank-Kopie
        self.create_database_copy(input_db, output_db)
        
        # 2. Öffne Output-Datenbank
        conn = duckdb.connect(output_db)
        
        # 3. Erstelle Chunks-Tabelle
        self.create_chunks_table(conn)
        
        # 4. Verarbeite Reden zu Chunks
        chunks = self.process_speeches_to_chunks(conn, max_speeches)
        
        # 5. Füge Chunks in Datenbank ein
        self.insert_chunks_to_database(conn, chunks)
        
        # 6. Erstelle Indizes
        self.create_indexes(conn)
        
        # 7. Statistiken
        self.show_statistics(conn, len(chunks))
        
        conn.close()
        print(f"\n✓ Chunking-Pipeline abgeschlossen!")
        print(f"✓ Neue Datenbank: {output_db}")
    
    def show_statistics(self, conn, total_chunks: int):
        """Zeigt Statistiken"""
        print("\n" + "=" * 70)
        print("STATISTIKEN")
        print("=" * 70)
        
        # Chunk-Statistiken
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_chunks,
                AVG(word_count) as avg_words,
                AVG(char_count) as avg_chars,
                MIN(word_count) as min_words,
                MAX(word_count) as max_words,
                MIN(char_count) as min_chars,
                MAX(char_count) as max_chars
            FROM chunks
        """).fetchone()
        
        print(f"Gesamt Chunks:        {stats[0]:,}")
        print(f"Durchschnitt Wörter:   {stats[1]:.1f}")
        print(f"Durchschnitt Zeichen:  {stats[2]:.1f}")
        print(f"Wort-Range:           {stats[3]}-{stats[4]}")
        print(f"Zeichen-Range:        {stats[5]}-{stats[6]}")
        
        # Speaker-Statistiken
        speaker_stats = conn.execute("""
            SELECT speaker_name, COUNT(*) as chunk_count
            FROM chunks 
            GROUP BY speaker_name 
            ORDER BY chunk_count DESC 
            LIMIT 10
        """).fetchall()
        
        print(f"\nTop 10 Speaker (Chunks):")
        for speaker, count in speaker_stats:
            print(f"  {speaker:30}: {count:6,}")
        
        # Chunking-Methode
        method_stats = conn.execute("""
            SELECT chunking_method, COUNT(*) as count
            FROM chunks 
            GROUP BY chunking_method
        """).fetchall()
        
        print(f"\nChunking-Methoden:")
        for method, count in method_stats:
            print(f"  {method:15}: {count:8,}")

def main():
    parser = argparse.ArgumentParser(description='Datenbank-basiertes intelligentes Chunking')
    parser.add_argument('--input-db', default=INPUT_DB, help='Pfad zur Input-Datenbank')
    parser.add_argument('--output-db', default=OUTPUT_DB, help='Pfad zur Output-Datenbank')
    parser.add_argument('--method', choices=['semantic'], default='semantic', help='Chunking-Methode')
    parser.add_argument('--max-chars', type=int, default=800, help='Maximale Zeichen pro Chunk')
    parser.add_argument('--max-speeches', type=int, help='Maximale Anzahl Reden (für Tests)')
    
    args = parser.parse_args()
    
    # Prüfe Input-Datenbank
    if not Path(args.input_db).exists():
        print(f"✗ Input-Datenbank {args.input_db} nicht gefunden!")
        return
    
    # Erstelle Chunker
    chunker = DatabaseChunker(args.method)
    
    # Führe Pipeline aus
    chunker.run_chunking_pipeline(args.input_db, args.output_db, args.max_speeches)

if __name__ == "__main__":
    main()
