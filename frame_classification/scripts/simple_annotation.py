#!/usr/bin/env python3
"""
Einfaches Terminal-basiertes Annotation Interface
Für die intelligenten semantic Chunks
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
import sys

# Frame-Kategorien
FRAME_CATEGORIES = [
    "Human Impact",
    "Powerlessness", 
    "Economic",
    "Moral Value",
    "Conflict",
    "Other"
]

class SimpleAnnotationInterface:
    def __init__(self, chunks_file: str, output_file: str):
        self.chunks_file = chunks_file
        self.output_file = output_file
        self.chunks = []
        self.current_index = 0
        self.annotations = {}
        
    def load_chunks(self):
        """Lädt Chunks aus JSON-Datei"""
        print(f"Lade intelligente Chunks aus {self.chunks_file}...")
        
        with open(self.chunks_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.chunks = data['chunks']
        
        print(f"✓ {len(self.chunks)} intelligente Chunks geladen")
        print(f"✓ Chunking-Methode: {data['metadata'].get('chunking_method', 'unknown')}")
        
        # Lade existierende Annotationen falls vorhanden
        if Path(self.output_file).exists():
            self.load_existing_annotations()
    
    def load_existing_annotations(self):
        """Lädt existierende Annotationen"""
        print(f"Lade existierende Annotationen aus {self.output_file}...")
        
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.annotations = data.get('annotations', {})
                self.current_index = data.get('current_index', 0)
            
            print(f"✓ {len(self.annotations)} existierende Annotationen geladen")
        except Exception as e:
            print(f"Warnung: Konnte existierende Annotationen nicht laden: {e}")
            self.annotations = {}
            self.current_index = 0
    
    def save_annotations(self):
        """Speichert Annotationen"""
        output_data = {
            'metadata': {
                'total_chunks': len(self.chunks),
                'annotated_chunks': len(self.annotations),
                'current_index': self.current_index,
                'frame_categories': FRAME_CATEGORIES,
                'chunking_method': 'semantic'
            },
            'annotations': self.annotations
        }
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"✓ Annotationen gespeichert nach {self.output_file}")
    
    def display_chunk(self, chunk: Dict[str, Any]):
        """Zeigt einen Chunk zur Annotation an"""
        print("\n" + "="*80)
        print(f"INTELLIGENTE CHUNK {self.current_index + 1} von {len(self.chunks)}")
        print("="*80)
        
        print(f"Chunk ID:     {chunk['chunk_id']}")
        print(f"Speaker:      {chunk['speaker_name']} ({chunk['speaker_party']})")
        print(f"Debate:       {chunk['debate_title']}")
        print(f"Date:         {chunk['debate_date']}")
        print(f"Words:        {chunk['word_count']}")
        print(f"Chars:        {chunk['char_count']}")
        print(f"Method:       {chunk['chunking_method']}")
        print(f"Chunk:        {chunk['chunk_index'] + 1}/{chunk['total_chunks']}")
        
        print("\n" + "-"*80)
        print("TEXT:")
        print("-"*80)
        
        # Zeige Text mit Zeilenumbrüchen für bessere Lesbarkeit
        text = chunk['chunk_text']
        words = text.split()
        lines = []
        current_line = []
        
        for word in words:
            current_line.append(word)
            if len(' '.join(current_line)) > 75:
                lines.append(' '.join(current_line))
                current_line = []
        
        if current_line:
            lines.append(' '.join(current_line))
        
        for line in lines:
            print(line)
        
        print("-"*80)
    
    def get_user_input(self) -> Optional[Dict[str, Any]]:
        """Holt Annotation vom Benutzer"""
        print("\nFRAME CLASSIFICATION:")
        print("Wähle eine Kategorie:")
        
        for i, category in enumerate(FRAME_CATEGORIES, 1):
            print(f"  {i}. {category}")
        
        print("\nKommandos:")
        print("  s - Speichern und beenden")
        print("  q - Beenden ohne speichern")
        print("  n - Nächster Chunk")
        print("  p - Vorheriger Chunk")
        print("  j <nummer> - Springe zu Chunk")
        print("  r - Chunk überspringen (keine Annotation)")
        
        while True:
            try:
                choice = input("\nDeine Wahl: ").strip().lower()
                
                if choice == 's':
                    return 'save'
                elif choice == 'q':
                    return 'quit'
                elif choice == 'n':
                    return 'next'
                elif choice == 'p':
                    return 'previous'
                elif choice == 'r':
                    return 'skip'
                elif choice.startswith('j '):
                    try:
                        target = int(choice.split()[1]) - 1
                        if 0 <= target < len(self.chunks):
                            return {'action': 'jump', 'target': target}
                        else:
                            print(f"Ungültige Chunk-Nummer. Bitte 1-{len(self.chunks)}")
                    except:
                        print("Ungültiges Format. Verwende 'j <nummer>'")
                elif choice.isdigit():
                    category_num = int(choice)
                    if 1 <= category_num <= len(FRAME_CATEGORIES):
                        frame_label = FRAME_CATEGORIES[category_num - 1]
                        
                        # Hole zusätzliche Informationen
                        confidence = self.get_confidence()
                        notes = self.get_notes()
                        
                        return {
                            'action': 'annotate',
                            'frame_label': frame_label,
                            'confidence': confidence,
                            'notes': notes
                        }
                    else:
                        print(f"Ungültige Kategorie. Bitte 1-{len(FRAME_CATEGORIES)}")
                else:
                    print("Ungültige Eingabe. Bitte versuche es erneut.")
                    
            except KeyboardInterrupt:
                print("\n\nBeendet durch Benutzer.")
                return 'quit'
            except Exception as e:
                print(f"Fehler: {e}")
    
    def get_confidence(self) -> int:
        """Holt Confidence-Level vom Benutzer"""
        while True:
            try:
                confidence = input("Confidence (1-5, Enter für 3): ").strip()
                if not confidence:
                    return 3
                confidence = int(confidence)
                if 1 <= confidence <= 5:
                    return confidence
                else:
                    print("Bitte 1-5 eingeben")
            except ValueError:
                print("Bitte eine Zahl eingeben")
    
    def get_notes(self) -> str:
        """Holt Notizen vom Benutzer"""
        notes = input("Notizen (optional, Enter zum Überspringen): ").strip()
        return notes if notes else ""
    
    def annotate_chunk(self, chunk: Dict[str, Any], annotation: Dict[str, Any]):
        """Speichert Annotation für einen Chunk"""
        chunk_id = chunk['chunk_id']
        self.annotations[chunk_id] = {
            'chunk_id': chunk_id,
            'speech_id': chunk['speech_id'],
            'speaker_name': chunk['speaker_name'],
            'speaker_party': chunk['speaker_party'],
            'frame_label': annotation['frame_label'],
            'confidence': annotation['confidence'],
            'notes': annotation['notes'],
            'chunk_text': chunk['chunk_text'],
            'chunking_method': chunk['chunking_method']
        }
        
        print(f"✓ Annotation gespeichert: {annotation['frame_label']}")
    
    def run(self):
        """Hauptschleife der Annotation"""
        self.load_chunks()
        
        if not self.chunks:
            print("Keine Chunks zum Annotieren gefunden!")
            return
        
        print(f"\nWillkommen zur intelligenten Frame-Classification Annotation!")
        print(f"Du beginnst bei Chunk {self.current_index + 1}")
        print(f"Bereits annotiert: {len(self.annotations)}")
        
        while self.current_index < len(self.chunks):
            chunk = self.chunks[self.current_index]
            
            # Zeige Chunk
            self.display_chunk(chunk)
            
            # Hole Benutzereingabe
            user_input = self.get_user_input()
            
            if user_input == 'quit':
                break
            elif user_input == 'save':
                self.save_annotations()
                break
            elif user_input == 'next':
                self.current_index += 1
            elif user_input == 'previous':
                self.current_index = max(0, self.current_index - 1)
            elif user_input == 'skip':
                self.current_index += 1
            elif isinstance(user_input, dict):
                if user_input['action'] == 'jump':
                    self.current_index = user_input['target']
                elif user_input['action'] == 'annotate':
                    self.annotate_chunk(chunk, user_input)
                    self.current_index += 1
        
        # Speichere am Ende
        self.save_annotations()
        
        # Zeige Statistiken
        self.show_statistics()
    
    def show_statistics(self):
        """Zeigt Annotation-Statistiken"""
        print("\n" + "="*80)
        print("ANNOTATION STATISTIKEN")
        print("="*80)
        
        total_chunks = len(self.chunks)
        annotated_chunks = len(self.annotations)
        progress = (annotated_chunks / total_chunks * 100) if total_chunks > 0 else 0
        
        print(f"Gesamt Chunks:        {total_chunks:,}")
        print(f"Annotiert:            {annotated_chunks:,}")
        print(f"Fortschritt:          {progress:.1f}%")
        
        if annotated_chunks > 0:
            print(f"\nFrame-Verteilung:")
            frame_counts = {}
            for annotation in self.annotations.values():
                frame = annotation['frame_label']
                frame_counts[frame] = frame_counts.get(frame, 0) + 1
            
            for frame, count in sorted(frame_counts.items()):
                percentage = (count / annotated_chunks * 100)
                print(f"  {frame:15}: {count:4} ({percentage:5.1f}%)")

def main():
    parser = argparse.ArgumentParser(description='Einfache Annotation für intelligente Chunks')
    parser.add_argument('--chunks', default='data/speech_chunks_semantic.json', help='Pfad zur chunks.json Datei')
    parser.add_argument('--output', default='annotations/annotations.json', help='Pfad zur Ausgabe-JSON Datei')
    parser.add_argument('--start-index', type=int, default=0, help='Start-Index für Annotation')
    
    args = parser.parse_args()
    
    # Prüfe Input-Datei
    if not Path(args.chunks).exists():
        print(f"✗ Chunks-Datei {args.chunks} nicht gefunden!")
        return
    
    # Erstelle Interface
    interface = SimpleAnnotationInterface(args.chunks, args.output)
    interface.current_index = args.start_index
    
    try:
        interface.run()
    except KeyboardInterrupt:
        print("\n\nAnnotation unterbrochen. Speichere Fortschritt...")
        interface.save_annotations()
    except Exception as e:
        print(f"\nFehler: {e}")
        interface.save_annotations()

if __name__ == "__main__":
    main()

