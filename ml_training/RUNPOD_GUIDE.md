# 🚀 RunPod Deployment Guide - GPT-OSS-20B Fine-Tuning

Komplette Anleitung für das Fine-Tuning von GPT-OSS-20B auf RunPod H100 GPU.

## 📋 Voraussetzungen

- RunPod Account (https://runpod.io)
- DuckDB Datei: `debates_brexit_chunked.duckdb` mit training_data Tabelle
- Weights & Biases Account (optional, für Monitoring)

## 🎯 Step-by-Step Anleitung

### Step 1: RunPod Pod Erstellen

1. **Einloggen** bei [RunPod.io](https://www.runpod.io/)
2. **Wähle GPU**: H100 SXM (80GB VRAM)
3. **Template auswählen**: 
   - `runpod/pytorch:2.1.0-py3.10-cuda12.1.1-devel`
   - Oder ein anderes PyTorch 2.x Template mit CUDA 12.x
4. **Storage**: Mindestens 100GB
5. **Pod starten**

### Step 2: Projekt Setup

#### Option A: Mit Git (empfohlen)

```bash
# SSH in Pod
ssh -p [PORT] root@[POD_IP]

# Repository klonen
cd /workspace
git clone [YOUR_REPO_URL]
cd ml_training
```

#### Option B: Manuelle Datei-Upload

1. Nutze den RunPod File Browser
2. Lade alle Dateien aus dem `ml_training/` Ordner hoch
3. Lade `debates_brexit_chunked.duckdb` in `data/` hoch

### Step 3: Environment Setup

```bash
cd /workspace/ml_training

# Python Dependencies installieren
pip install -r requirements.txt

# Setup-Script ausführbar machen
chmod +x runpod_setup.sh

# Setup ausführen (prüft System, GPU, etc.)
./runpod_setup.sh
```

### Step 4: Weights & Biases (Optional)

```bash
# API Key als Environment Variable setzen
export WANDB_API_KEY="your_wandb_api_key_here"

# Oder interaktiv einloggen
wandb login
```

Ohne W&B läuft das Training trotzdem, nur ohne Online-Monitoring.

### Step 5: Training Starten

```bash
# Training starten
python3 src/train.py

# Das Training läuft nun und zeigt Progress in der Konsole
```

**Erwartete Ausgabe:**
```
Lade Daten aus debates_brexit_chunked.duckdb...
Train Samples: XXXX
Test Samples: XXXX
Anzahl Labels: 6
Label Namen: ['label_1', 'label_2', ...]

Lade Model: openai-community/gpt-oss-20b
Konfiguriere LoRA...
trainable params: 83,886,080 || all params: 20,083,886,080 || trainable%: 0.4177

==================================================
Starte Training...
==================================================
```

### Step 6: Monitoring (in separatem Terminal)

**Terminal 1: Training läuft**

**Terminal 2: GPU Monitoring**
```bash
# SSH in zweitem Terminal
ssh -p [PORT] root@[POD_IP]

# GPU Stats in Echtzeit
watch -n 1 nvidia-smi
```

**Terminal 3: TensorBoard (optional)**
```bash
ssh -p [PORT] root@[POD_IP]
cd /workspace/ml_training
tensorboard --logdir outputs --port 6006 --host 0.0.0.0
```

Dann öffne: `http://[POD_IP]:6006`

## ⏱️ Training Duration

Auf H100 SXM:
- **3 Epochs**: ~3-6 Stunden (abhängig von Datengröße)
- **Memory Usage**: ~45-55GB VRAM
- **Checkpoints**: Automatisch alle 500 Steps

## 📊 Training Outputs

Nach dem Training findest du:

```
ml_training/
├── outputs/                          # Training Logs & TensorBoard
│   ├── trainer_state.json
│   ├── training_args.bin
│   └── [weitere Logs]
├── checkpoints/
│   ├── checkpoint-500/              # Zwischenspeicherungen
│   ├── checkpoint-1000/
│   └── final_model/                 # Finales trainiertes Model
│       ├── config.json
│       ├── adapter_model.bin        # LoRA Weights
│       ├── adapter_config.json
│       └── [weitere Dateien]
```

## 🎯 Nach dem Training

### Evaluation durchführen

```bash
python3 src/evaluate.py
```

Dies erstellt `evaluation_results/` mit:
- `metrics.json` - Detaillierte Metriken
- `confusion_matrices.png` - Visualisierungen
- `label_distribution.png` - Label-Verteilung

### Test Inference

```bash
python3 src/inference.py
```

Oder im Python Script:
```python
from src.inference import DebateClassifier

classifier = DebateClassifier(
    checkpoint_path="checkpoints/final_model",
    threshold=0.5
)

text = "Brexit hat die Beziehung zwischen UK und EU verändert."
result = classifier.predict(text)
print(result['predicted_labels'])
print(result['label_scores'])
```

## 💾 Checkpoints Herunterladen

### Option 1: RunPod File Browser
1. Navigiere zu `checkpoints/final_model/`
2. Download alle Dateien

### Option 2: SCP
```bash
# Auf deinem lokalen Computer:
scp -P [PORT] -r root@[POD_IP]:/workspace/ml_training/checkpoints/final_model ./local_checkpoints/
```

### Option 3: Cloud Storage
```bash
# In RunPod Terminal:
pip install awscli  # oder gsutil für GCP

# Upload zu S3
aws s3 cp checkpoints/final_model/ s3://your-bucket/models/ --recursive

# Oder zu Google Cloud
gsutil -m cp -r checkpoints/final_model/ gs://your-bucket/models/
```

## 🔧 Konfiguration Anpassen

Bearbeite `config/training_config.yaml`:

```yaml
training:
  num_epochs: 3              # Mehr Epochs für bessere Performance
  batch_size: 4              # Reduzieren bei OOM
  learning_rate: 2e-4        # Anpassen für bessere Konvergenz
  
  # Bei Memory-Problemen:
  batch_size: 2
  gradient_accumulation_steps: 16  # Behält effektive Batch Size bei
```

## 🐛 Troubleshooting

### Out of Memory (OOM)

**Symptom:** `CUDA out of memory` Error

**Lösung:**
```yaml
# In training_config.yaml:
training:
  batch_size: 2              # Von 4 auf 2
  gradient_accumulation_steps: 16  # Von 8 auf 16
```

### DuckDB Connection Error

**Symptom:** `duckdb.IOException: Cannot open file`

**Lösung:**
```bash
# Prüfe Dateipfad
ls -lh data/debates_brexit_chunked.duckdb

# Prüfe Permissions
chmod 644 data/debates_brexit_chunked.duckdb
```

### Model Loading Failed

**Symptom:** `OSError: Model openai-community/gpt-oss-20b not found`

**Lösung:**
```bash
# Manuell Model herunterladen
from transformers import AutoModel
model = AutoModel.from_pretrained("openai-community/gpt-oss-20b", cache_dir="./models")
```

### Training sehr langsam

**Check:**
```bash
# GPU Auslastung prüfen
nvidia-smi

# Falls GPU Utilization < 70%:
# - Erhöhe batch_size
# - Prüfe ob BF16 aktiviert ist (sollte true sein)
# - Stelle sicher dass CUDA richtig konfiguriert ist
python3 -c "import torch; print(torch.cuda.is_available())"
```

## 📈 Performance Erwartungen

### Mit Standard-Config (H100):

| Metric | Wert |
|--------|------|
| Training Time | 3-6h für 3 Epochs |
| VRAM Usage | 45-55GB |
| GPU Utilization | 80-95% |
| Samples/Sec | ~15-25 |
| Trainable Params | 84M (0.4%) |

### Erwartete Metriken (nach Training):

| Metric | Erwarteter Bereich |
|--------|-------------------|
| F1 Macro | 0.65 - 0.85 |
| F1 Micro | 0.70 - 0.90 |
| Accuracy | 0.75 - 0.95 |

## 💰 Kosten-Kalkulation

RunPod H100 SXM:
- **Preis**: ~$4.00/Stunde (variiert)
- **Training (6h)**: ~$24
- **Mit Idle Time**: +20% Buffer = ~$30

**Tipp**: Stoppe den Pod sofort nach Download der Checkpoints!

## 🔐 Best Practices

1. **Regelmäßige Checkpoints**: Alle 500 Steps (bereits konfiguriert)
2. **Monitoring**: Nutze W&B oder TensorBoard
3. **Backup**: Lade Checkpoints während Training hoch (nicht warten bis Ende)
4. **Pod Management**: Stoppe Pod wenn nicht in Verwendung
5. **Screen/Tmux**: Nutze für lange Training Sessions
   ```bash
   tmux new -s training
   python3 src/train.py
   # Ctrl+B, dann D zum Detachen
   # tmux attach -t training zum Wiederverbinden
   ```

## 📞 Support

Bei Problemen:
1. Check Logs in `outputs/`
2. Prüfe `nvidia-smi` für GPU Status
3. Teste mit kleinerem Subset der Daten
4. Überprüfe DuckDB Daten-Schema

## 🎓 Nächste Schritte

Nach erfolgreichem Training:
1. ✅ Evaluation durchführen
2. ✅ Checkpoints downloaden
3. ✅ Model in Production deployen
4. ✅ Inference auf neuen Daten testen
5. ✅ Hyperparameter-Tuning für bessere Performance

---

**Happy Training! 🚀**
