# Frame Classification für Brexit-Debatten

Dieses Projekt ermöglicht die Klassifikation von politischen Reden in verschiedene Frames mithilfe von LLM-Fine-Tuning.

## Frame-Kategorien

- **Human Impact**: Fokus auf menschliche Auswirkungen, persönliche Geschichten, Betroffenheit
- **Powerlessness**: Gefühl der Ohnmacht, fehlende Kontrolle, Hilflosigkeit  
- **Economic**: Wirtschaftliche Argumente, Kosten, Nutzen, Finanzen
- **Moral Value**: Ethische/moralische Argumente, Werte, Prinzipien
- **Conflict**: Konflikt, Opposition, Widerstand, Kampf
- **Other**: Sonstige Kategorien, die nicht in die obigen passen

## Projektstruktur

```
frame_classification/
├── data/                    # Verarbeitete Daten
├── scripts/                 # Python-Scripts
│   ├── load_speeches.py     # Lädt Reden und erstellt Chunks
│   ├── annotation_interface.py  # Interaktive Annotation
│   ├── generate_training_data.py  # Generiert Training-Daten
│   └── fine_tuning_pipeline.py   # Fine-Tuning Pipeline
├── annotations/             # Annotation-Ergebnisse
├── models/                  # Trainierte Modelle
└── README.md
```

## Workflow

### 1. Daten vorbereiten

```bash
# Lade Reden aus gefilterter Datenbank und erstelle Chunks
python scripts/load_speeches.py \
    --chunks ../../data/processed/debates_brexit_filtered.duckdb \
    --output-dir data \
    --chunk-size 500 \
    --overlap 50
```

### 2. Chunking durchführen

#### Intelligentes Datenbank-Chunking

```bash
# Erstelle intelligente Chunks in der Datenbank
python scripts/simple_database_chunking.py --max-speeches 1000

# Für alle Daten (kann lange dauern)
python scripts/simple_database_chunking.py
```

**Chunking-Features:**
- 🧠 Semantisches Chunking mit spaCy
- 📊 Optimale Chunk-Größe (100-150 Wörter)
- 🗄️ Direkte Speicherung in DuckDB
- 👤 User-Zuweisung für Annotation
- 📈 Vollständige Metadaten

### 3. Annotation durchführen

#### Datenbank-basiertes Streamlit Interface (Empfohlen)

```bash
# Starte Streamlit Annotation Interface
python start_annotation_db.py
```

**Streamlit Interface Features:**
- 🌐 Web-basiertes Interface (http://localhost:8501)
- 🗄️ Direkte Datenbank-Integration
- 📊 Live-Statistiken und Fortschrittsanzeige
- 💾 Automatisches Speichern in DB
- 🔄 Pausieren und später weitermachen
- 📈 Frame-Verteilung Charts
- 👤 User-Management
- 📥 CSV/JSON-Export für Backup
- 🧭 Einfache Navigation zwischen Chunks

#### Option B: Terminal Interface

```bash
# Starte Terminal Annotation
python scripts/annotation_interface.py \
    --chunks data/speech_chunks.json \
    --output annotations/annotations.json
```

**Terminal Interface Features:**
- Zeigt Chunks mit Metadaten an
- Einfache Tastatur-Navigation
- Speichert Fortschritt automatisch
- Statistiken und Verteilung

**Kommandos:**
- `1-6`: Frame-Kategorie auswählen
- `s`: Speichern und beenden
- `q`: Beenden ohne speichern
- `n`: Nächster Chunk
- `p`: Vorheriger Chunk
- `j <nummer>`: Springe zu Chunk
- `r`: Chunk überspringen

### 3. Training-Daten generieren

```bash
# Generiere Training-Daten in verschiedenen Formaten
python scripts/generate_training_data.py \
    --annotations annotations/annotations.json \
    --output-dir data/training_data \
    --test-size 0.2
```

**Generierte Formate:**
- `train.jsonl` / `test.jsonl`: JSONL-Format für Classification
- `train.csv` / `test.csv`: CSV-Format
- `train_alpaca.jsonl` / `test_alpaca.jsonl`: Alpaca-Format für Instruction-Following
- `few_shot_examples.json`: Beste Beispiele pro Kategorie
- `prompt_template.txt`: Prompt-Template für Few-Shot

### 4. Fine-Tuning durchführen

#### Option A: Hugging Face (Empfohlen)

```bash
# Bereite Hugging Face Fine-Tuning vor
python scripts/fine_tuning_pipeline.py \
    --training-data data/training_data \
    --output-dir models \
    --approach huggingface \
    --model-name distilbert-base-uncased

# Führe Fine-Tuning aus
python models/hf_fine_tuning.py
```

#### Option B: OpenAI

```bash
# OpenAI Fine-Tuning (erfordert API-Key)
export OPENAI_API_KEY="your-api-key"

python scripts/fine_tuning_pipeline.py \
    --training-data data/training_data \
    --output-dir models \
    --approach openai
```

### 5. Modell evaluieren

```bash
# Evaluierung des trainierten Modells
python models/evaluate_model.py \
    --model-path models/frame_classification_model \
    --test-data data/training_data/test.jsonl \
    --output results/evaluation.json
```

## Verwendung des trainierten Modells

```python
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# Lade Modell
tokenizer = AutoTokenizer.from_pretrained("models/frame_classification_model")
model = AutoModelForSequenceClassification.from_pretrained("models/frame_classification_model")

# Klassifiziere Text
def classify_frame(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    
    with torch.no_grad():
        outputs = model(**inputs)
        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_class_id = predictions.argmax().item()
        confidence = predictions.max().item()
    
    id2label = model.config.id2label
    return id2label[predicted_class_id], confidence

# Beispiel
text = "Die Menschen in meinem Wahlkreis sind besorgt über die wirtschaftlichen Auswirkungen..."
frame, confidence = classify_frame(text)
print(f"Frame: {frame} (Confidence: {confidence:.3f})")
```

## Dependencies

```bash
# Basis-Dependencies
pip install duckdb transformers torch

# Für OpenAI Fine-Tuning
pip install openai

# Für Hugging Face Fine-Tuning
pip install datasets scikit-learn

# Für Evaluation
pip install numpy pandas
```

## Tipps für bessere Ergebnisse

### Annotation
- **Konsistenz**: Verwende die gleichen Kriterien für ähnliche Texte
- **Confidence**: Gib ehrliche Confidence-Werte (1-5)
- **Notizen**: Dokumentiere schwierige Fälle
- **Qualität**: Mindestens 50-100 Annotationen pro Kategorie empfohlen

### Fine-Tuning
- **Datenqualität**: Saubere, konsistente Annotationen sind wichtiger als Menge
- **Balance**: Versuche ausgewogene Verteilung der Kategorien
- **Validation**: Verwende Test-Set für unabhängige Evaluation
- **Iteration**: Verbessere Annotationen basierend auf Modell-Fehlern

### Modell-Auswahl
- **DistilBERT**: Schnell, gut für kleinere Datasets
- **BERT-base**: Ausgewogen zwischen Performance und Geschwindigkeit
- **RoBERTa**: Besser für komplexere Patterns
- **DeBERTa**: State-of-the-art für Text-Klassifikation

## Troubleshooting

### Häufige Probleme

1. **Zu wenige Annotationen**
   - Mindestens 10-20 pro Kategorie
   - Verwende Few-Shot Learning für kleine Datasets

2. **Unausgewogene Daten**
   - Verwende stratifizierte Stichprobe
   - Oversampling für seltene Kategorien

3. **Schlechte Performance**
   - Überprüfe Annotation-Qualität
   - Experimentiere mit verschiedenen Modellen
   - Erhöhe Training-Epochs

4. **Memory-Probleme**
   - Reduziere Batch-Size
   - Verwende Gradient-Checkpointing
   - Verwende kleinere Modelle

## Erweiterte Features

### Batch-Processing
```bash
# Verarbeite mehrere Chunks gleichzeitig
python scripts/load_speeches.py --max-speeches 100
```

### Custom Frames
Bearbeite `FRAME_CATEGORIES` in den Scripts für eigene Kategorien.

### Multi-Label Classification
Erweitere die Scripts für mehrere Labels pro Text.

### Active Learning
Implementiere Active Learning für effizientere Annotation.

## Lizenz

Dieses Projekt ist Teil des Brexit-Debatten-Analyse-Projekts.
