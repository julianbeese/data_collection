# ML Training & Fine-Tuning

## 📁 Struktur

### `scripts/` - Training Skripte
- **`fine_tuning_pipeline.py`** - Kompletter Fine-Tuning Pipeline für Frame Classification
- **`generate_training_data.py`** - Training Data Generation aus annotierten Chunks

### `models/` - Modelle
- **Trained Models** - Fine-tuned Modelle
- **Checkpoints** - Training Checkpoints
- **Configs** - Model-Konfigurationen

### `data/` - Training Daten
- **Training Sets** - Aufbereitete Trainingsdaten
- **Validation Sets** - Validierungsdaten
- **Test Sets** - Testdaten

### `docs/` - Dokumentation
- **Training Logs** - Training-Verläufe
- **Model Cards** - Model-Dokumentation
- **Performance Reports** - Evaluationsergebnisse

## 🚀 Workflow

### 1. Training Data Generation
```bash
python scripts/generate_training_data.py --input-db ../data/processed/debates_brexit_chunked_final.duckdb
```

### 2. Fine-Tuning
```bash
python scripts/fine_tuning_pipeline.py --data-dir data/ --model-name "bert-base-german-cased"
```

## 📊 Features

- **Frame Classification** - Multi-label Classification
- **German BERT** - Optimiert für deutsche Texte
- **Transfer Learning** - Von pre-trained zu domain-specific
- **Evaluation** - Umfassende Metriken und Reports
