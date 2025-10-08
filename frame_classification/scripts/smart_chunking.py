#!/usr/bin/env python3
"""
Intelligentes Chunking für Frame-Classification
Verschiedene Ansätze: Absätze, spaCy-Sätze, semantische Chunks
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
import argparse

# Konfiguration
INPUT_DB = "../../data/processed/debates_brexit_filtered.duckdb"
OUTPUT_DIR = "../data"

class SmartChunker:
    def __init__(self, method: str = "spacy"):
        self.method = method
        self.nlp = None
        
        if method == "spacy":
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
    
    def chunk_by_paragraphs(self, text: str, max_chars: int = 1000) -> List[str]:
        """Chunking nach Absätzen"""
        if not text:
            return []
        
        # Teile nach doppelten Zeilenumbrüchen (Absätze)
        paragraphs = re.split(r'\n\s*\n', text)
        chunks = []
        current_chunk = ""
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            
            # Wenn Absatz zu lang, teile ihn
            if len(paragraph) > max_chars:
                # Teile langen Absatz in Sätze
                sentences = self.split_into_sentences(paragraph)
                for sentence in sentences:
                    if len(current_chunk + " " + sentence) <= max_chars:
                        current_chunk += " " + sentence if current_chunk else sentence
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        current_chunk = sentence
            else:
                # Normaler Absatz
                if len(current_chunk + " " + paragraph) <= max_chars:
                    current_chunk += " " + paragraph if current_chunk else paragraph
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = paragraph
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def chunk_by_spacy_sentences(self, text: str, max_sentences: int = 3) -> List[str]:
        """Chunking nach spaCy-Sätzen"""
        if not text or not self.nlp:
            return [text] if text else []
        
        doc = self.nlp(text)
        sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
        
        chunks = []
        current_chunk = []
        
        for sentence in sentences:
            current_chunk.append(sentence)
            
            if len(current_chunk) >= max_sentences:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        return chunks
    
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
    
    def chunk_text(self, text: str, method: str = None) -> List[str]:
        """Hauptfunktion für Chunking"""
        if not text:
            return []
        
        method = method or self.method
        
        if method == "paragraphs":
            return self.chunk_by_paragraphs(text)
        elif method == "spacy":
            return self.chunk_by_spacy_sentences(text)
        elif method == "semantic":
            return self.chunk_by_semantic_boundaries(text)
        else:
            # Fallback: Einfaches Wort-Chunking
            words = text.split()
            chunks = []
            for i in range(0, len(words), 100):
                chunk = " ".join(words[i:i+100])
                if chunk:
                    chunks.append(chunk)
            return chunks
    
    def analyze_chunk_quality(self, chunks: List[str]) -> Dict[str, Any]:
        """Analysiert Chunk-Qualität"""
        if not chunks:
            return {}
        
        word_counts = [len(chunk.split()) for chunk in chunks]
        char_counts = [len(chunk) for chunk in chunks]
        
        return {
            'total_chunks': len(chunks),
            'avg_words': sum(word_counts) / len(word_counts),
            'avg_chars': sum(char_counts) / len(char_counts),
            'min_words': min(word_counts),
            'max_words': max(word_counts),
            'min_chars': min(char_counts),
            'max_chars': max(char_counts)
        }

def load_speeches_from_db(db_path: str) -> List[Dict[str, Any]]:
    """Lädt alle Reden aus der Datenbank"""
    print(f"Lade Reden aus {db_path}...")
    
    conn = duckdb.connect(db_path, read_only=True)
    
    # Lade alle Speeches mit Metadaten
    query = """
    SELECT 
        s.speech_id,
        s.debate_id,
        s.speaker_name,
        s.speaker_office,
        s.speech_text,
        s.time,
        s.time,
        d.major_heading_text,
        d.date
    FROM speeches s
    LEFT JOIN debates d ON s.debate_id = d.debate_id
    ORDER BY s.speech_id
    """
    
    speeches = conn.execute(query).fetchall()
    
    # Konvertiere zu Dictionary-Liste
    speech_data = []
    for row in speeches:
        speech_data.append({
            'speech_id': row[0],
            'debate_id': row[1],
            'speaker_name': row[2],
            'speaker_party': row[3],  # speaker_office wird als party verwendet
            'speech_text': row[4] if row[4] else "",
            'start_time': row[5],
            'end_time': row[6],
            'debate_title': row[7],
            'debate_date': str(row[8]) if row[8] else None
        })
    
    conn.close()
    
    print(f"✓ {len(speech_data)} Reden geladen")
    return speech_data

def create_smart_chunks(speeches: List[Dict[str, Any]], chunker: SmartChunker, method: str) -> List[Dict[str, Any]]:
    """Erstellt intelligente Chunks aus den Reden"""
    print(f"Erstelle intelligente Chunks mit Methode: {method}...")
    
    all_chunks = []
    chunk_id = 0
    
    for speech in speeches:
        if not speech['speech_text']:
            continue
        
        # Bereinige Text
        clean_text = chunker.clean_text(speech['speech_text'])
        if not clean_text:
            continue
        
        # Erstelle Chunks
        chunks = chunker.chunk_text(clean_text, method)
        
        for i, chunk_text in enumerate(chunks):
            if not chunk_text.strip():
                continue
            
            chunk_data = {
                'chunk_id': f"chunk_{chunk_id:06d}",
                'speech_id': speech['speech_id'],
                'debate_id': speech['debate_id'],
                'speaker_name': speech['speaker_name'],
                'speaker_party': speech['speaker_party'],
                'debate_title': speech['debate_title'],
                'debate_date': speech['debate_date'],
                'chunk_text': chunk_text,
                'chunk_index': i,
                'total_chunks': len(chunks),
                'word_count': len(chunk_text.split()),
                'char_count': len(chunk_text),
                'chunking_method': method,
                'frame_label': None,
                'annotation_confidence': None,
                'annotation_notes': None
            }
            
            all_chunks.append(chunk_data)
            chunk_id += 1
    
    print(f"✓ {len(all_chunks)} intelligente Chunks erstellt")
    return all_chunks

def save_chunks_to_json(chunks: List[Dict[str, Any]], output_path: str, method: str):
    """Speichert Chunks als JSON-Datei"""
    print(f"Speichere Chunks nach {output_path}...")
    
    output_data = {
        'metadata': {
            'total_chunks': len(chunks),
            'chunking_method': method,
            'frame_categories': [
                "Human Impact",
                "Powerlessness", 
                "Economic",
                "Moral Value",
                "Conflict",
                "Other"
            ],
            'created_at': str(Path().cwd())
        },
        'chunks': chunks
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Chunks gespeichert")

def create_annotation_template(chunks: List[Dict[str, Any]], output_path: str):
    """Erstellt eine CSV-Vorlage für die Annotation"""
    print(f"Erstelle Annotation-Template nach {output_path}...")
    
    import csv
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'chunk_id',
            'speech_id', 
            'speaker_name',
            'speaker_party',
            'debate_title',
            'debate_date',
            'chunk_text',
            'word_count',
            'char_count',
            'chunking_method',
            'frame_label',
            'annotation_confidence',
            'annotation_notes'
        ])
        
        # Daten
        for chunk in chunks:
            writer.writerow([
                chunk['chunk_id'],
                chunk['speech_id'],
                chunk['speaker_name'],
                chunk['speaker_party'],
                chunk['debate_title'],
                chunk['debate_date'],
                chunk['chunk_text'],
                chunk['word_count'],
                chunk['char_count'],
                chunk['chunking_method'],
                '',  # frame_label - leer für manuelle Annotation
                '',  # annotation_confidence
                ''   # annotation_notes
            ])
    
    print(f"✓ Annotation-Template erstellt")

def main():
    parser = argparse.ArgumentParser(description='Intelligentes Chunking für Frame-Classification')
    parser.add_argument('--input-db', default=INPUT_DB, help='Pfad zur gefilterten Datenbank')
    parser.add_argument('--output-dir', default=OUTPUT_DIR, help='Ausgabeverzeichnis')
    parser.add_argument('--method', choices=['paragraphs', 'spacy', 'semantic'], 
                       default='semantic', help='Chunking-Methode')
    parser.add_argument('--max-speeches', type=int, help='Maximale Anzahl Reden (für Tests)')
    parser.add_argument('--analyze', action='store_true', help='Analysiere Chunk-Qualität')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("INTELLIGENTES CHUNKING FÜR FRAME CLASSIFICATION")
    print("=" * 70)
    
    # Prüfe Input-Datenbank
    if not Path(args.input_db).exists():
        print(f"\n✗ Input-Datenbank {args.input_db} nicht gefunden!")
        return
    
    # Erstelle Output-Verzeichnis
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Erstelle Chunker
    chunker = SmartChunker(args.method)
    
    # Lade Reden
    speeches = load_speeches_from_db(args.input_db)
    
    # Begrenze Anzahl für Tests
    if args.max_speeches:
        speeches = speeches[:args.max_speeches]
        print(f"Begrenzt auf {args.max_speeches} Reden für Test")
    
    # Erstelle intelligente Chunks
    chunks = create_smart_chunks(speeches, chunker, args.method)
    
    # Analysiere Chunk-Qualität
    if args.analyze:
        print("\n" + "=" * 50)
        print("CHUNK-QUALITÄTSANALYSE")
        print("=" * 50)
        
        quality = chunker.analyze_chunk_quality([chunk['chunk_text'] for chunk in chunks])
        for key, value in quality.items():
            print(f"{key:15}: {value}")
    
    # Speichere als JSON
    json_path = Path(args.output_dir) / f"speech_chunks_{args.method}.json"
    save_chunks_to_json(chunks, str(json_path), args.method)
    
    # Erstelle Annotation-Template
    csv_path = Path(args.output_dir) / f"annotation_template_{args.method}.csv"
    create_annotation_template(chunks, str(csv_path))
    
    # Statistiken
    print("\n" + "=" * 70)
    print("STATISTIKEN")
    print("=" * 70)
    print(f"Chunking-Methode:     {args.method}")
    print(f"Reden verarbeitet:    {len(speeches):,}")
    print(f"Chunks erstellt:      {len(chunks):,}")
    print(f"Durchschnittliche Chunks pro Rede: {len(chunks)/max(len(speeches),1):.1f}")
    print(f"\nAusgabedateien:")
    print(f"  JSON:                {json_path}")
    print(f"  CSV Template:         {csv_path}")
    
    print(f"\n✓ Intelligentes Chunking abgeschlossen!")
    print(f"\nChunking-Methoden:")
    print(f"  paragraphs:  Nach Absätzen (gut für strukturierte Texte)")
    print(f"  spacy:      Nach spaCy-Sätzen (linguistisch korrekt)")
    print(f"  semantic:   Nach semantischen Grenzen (empfohlen)")

if __name__ == "__main__":
    main()

