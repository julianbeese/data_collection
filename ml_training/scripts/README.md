# ML Training Scripts

## 📋 Skripte

### `fine_tuning_pipeline.py`
**Kompletter Fine-Tuning Pipeline für Frame Classification**

**Features:**
- German BERT Fine-Tuning
- Multi-label Frame Classification
- Transfer Learning Pipeline
- Comprehensive Evaluation
- Model Checkpointing
- Performance Metrics

**Verwendung:**
```bash
python fine_tuning_pipeline.py --data-dir ../data/ --model-name "bert-base-german-cased"
```

### `generate_training_data.py`
**Training Data Generation aus annotierten Chunks**

**Features:**
- DuckDB Integration
- Frame Label Extraction
- Train/Validation/Test Split
- Data Preprocessing
- Export für Training

**Verwendung:**
```bash
python generate_training_data.py --input-db ../../data/processed/debates_brexit_chunked_final.duckdb
```

## 🔄 Workflow

1. **Data Generation** → `generate_training_data.py`
2. **Fine-Tuning** → `fine_tuning_pipeline.py`
3. **Evaluation** → Automatisch im Pipeline
4. **Model Export** → `../models/`

## 📊 Output

- **Trained Models** → `../models/`
- **Training Logs** → `../docs/`
- **Performance Reports** → `../docs/`
