# 🚀 Railway Deployment Guide - Frame Classification

**Kostengünstigste Lösung für 1 Woche + 5 User: ~$5-10 total!**

## 🎯 Warum Railway?

- ✅ **Sehr günstig**: $5-10 für die ganze Woche
- ✅ **PostgreSQL inklusive**: Keine separaten DB-Kosten
- ✅ **Einfaches Deployment**: GitHub → Railway
- ✅ **Multi-User**: PostgreSQL unterstützt gleichzeitige Zugriffe
- ✅ **Skalierbar**: Automatische Skalierung
- ✅ **Keine Infrastruktur-Verwaltung**

## 💰 Kosten-Übersicht

| Service | Kosten |
|---------|--------|
| **Railway Pro Plan** | $5/Monat (kann nach 1 Woche gekündigt werden) |
| **PostgreSQL** | Inklusive |
| **Deployment** | Kostenlos |
| **Gesamt für 1 Woche** | **~$1.25** |

## 🚀 Schritt-für-Schritt Deployment

### 1. Railway Account erstellen

1. Gehe zu [railway.app](https://railway.app)
2. Erstelle Account mit GitHub
3. Verbinde dein GitHub Repository

### 2. Projekt erstellen

```bash
# Installiere Railway CLI (optional)
npm install -g @railway/cli

# Login
railway login

# Erstelle neues Projekt
railway new
```

### 3. PostgreSQL Datenbank hinzufügen

1. In Railway Dashboard: **"New"** → **"Database"** → **"PostgreSQL"**
2. Warte bis Datenbank erstellt ist
3. Kopiere die `DATABASE_URL` aus den Umgebungsvariablen

### 4. App deployen

1. **"New"** → **"GitHub Repo"**
2. Wähle dein Repository
3. Railway erkennt automatisch Python + Streamlit
4. Setze **Root Directory** auf `frame_classification`
5. Setze **Start Command** auf:
   ```bash
   streamlit run streamlit_annotation_railway.py --server.port=$PORT --server.address=0.0.0.0
   ```

### 5. Umgebungsvariablen setzen

Railway setzt automatisch:
- `DATABASE_URL` (PostgreSQL Verbindung)
- `PORT` (für Streamlit)

### 6. Daten migrieren

```bash
# Lokal: Migriere DuckDB zu Railway PostgreSQL
python scripts/migrate_to_postgresql.py \
    --duckdb ../../data/processed/debates_brexit_chunked.duckdb \
    --host YOUR_RAILWAY_DB_HOST \
    --port 5432 \
    --database YOUR_RAILWAY_DB_NAME \
    --user YOUR_RAILWAY_DB_USER \
    --password YOUR_RAILWAY_DB_PASSWORD
```

## 🔧 Konfiguration

### Railway.toml

```toml
[build]
builder = "NIXPACKS"

[deploy]
startCommand = "streamlit run streamlit_annotation_railway.py --server.port=$PORT --server.address=0.0.0.0"
healthcheckPath = "/"
healthcheckTimeout = 300
restartPolicyType = "ON_FAILURE"
restartPolicyMaxRetries = 10
```

### Requirements.txt

```txt
streamlit>=1.28.0
psycopg2-binary>=2.9.0
pandas>=2.0.0
plotly>=5.15.0
```

## 📊 Monitoring

### Railway Dashboard

- **Metrics**: CPU, Memory, Requests
- **Logs**: Live-Logs der App
- **Database**: PostgreSQL Status
- **Deployments**: Deployment-History

### Kosten überwachen

```bash
# Zeige aktuelle Nutzung
railway status

# Zeige Logs
railway logs
```

## 🔒 Sicherheit

### Empfohlene Einstellungen

1. **Private Repository**: Nutze private GitHub Repos
2. **Environment Variables**: Sensitive Daten in Railway Secrets
3. **Database**: Automatische Backups inklusive
4. **HTTPS**: Automatisch aktiviert

## 🚨 Troubleshooting

### Häufige Probleme

1. **App startet nicht**
   ```bash
   # Prüfe Logs
   railway logs
   
   # Prüfe Requirements
   pip install -r requirements_railway.txt
   ```

2. **Datenbank-Verbindung fehlgeschlagen**
   ```bash
   # Prüfe DATABASE_URL
   railway variables
   
   # Teste Verbindung lokal
   python -c "import psycopg2; print('OK')"
   ```

3. **Performance-Probleme**
   - Erhöhe Railway Plan (Pro → Team)
   - Optimiere Datenbank-Queries
   - Reduziere Chunk-Limit

### Debugging

```bash
# Lokaler Test
export DATABASE_URL="postgresql://user:pass@host:port/db"
streamlit run streamlit_annotation_railway.py
```

## 📈 Skalierung

### Automatische Skalierung

- **Railway**: Skaliert automatisch je nach Traffic
- **PostgreSQL**: Automatische Connection Pooling
- **Memory**: Automatische Memory-Optimierung

### Manuelle Skalierung

```bash
# Erhöhe Railway Plan
# Pro → Team → Enterprise
# Mehr CPU, Memory, Database Connections
```

## 🔄 Updates & Maintenance

### App Updates

```bash
# Automatisches Update via GitHub
git push origin main
# Railway deployt automatisch
```

### Datenbank Backups

- **Automatisch**: Railway macht tägliche Backups
- **Manuell**: Export via Railway Dashboard
- **Download**: Backup-Dateien herunterladen

## 💡 Kosten-Optimierung

### Nach 1 Woche

1. **Kündige Railway Pro Plan**
2. **Exportiere Daten** (falls nötig)
3. **Lösche Projekt** (optional)

### Free Tier nutzen

- **Railway Free**: 500 Stunden/Monat
- **PostgreSQL Free**: 1GB Storage
- **Für kleine Projekte ausreichend**

## 📞 Support

### Railway Support

- **Documentation**: [docs.railway.app](https://docs.railway.app)
- **Discord**: Railway Community
- **GitHub Issues**: Railway Repository

### Debugging Tools

```bash
# Railway CLI
railway status
railway logs
railway connect

# Lokale Entwicklung
railway run streamlit run streamlit_annotation_railway.py
```

## 🎉 Erfolg!

Nach erfolgreichem Deployment hast du:

- ✅ **Multi-User Annotation Interface**
- ✅ **PostgreSQL Datenbank**
- ✅ **Automatische Skalierung**
- ✅ **HTTPS-Sicherheit**
- ✅ **Monitoring & Logs**
- ✅ **Kostengünstige Lösung (~$5-10 für 1 Woche)**

**App URL**: `https://your-app-name.railway.app`

## 🏆 Vergleich der Optionen

| Option | Kosten/Woche | Setup | Multi-User | Skalierung |
|--------|--------------|-------|------------|------------|
| **Railway** | $1.25 | ⭐⭐⭐ | ✅ | ✅ |
| **Render** | $1.75 | ⭐⭐⭐ | ✅ | ✅ |
| **GCP Cloud Run** | $2.50 | ⭐⭐ | ✅ | ✅ |
| **Streamlit Cloud** | $0 | ⭐⭐⭐⭐ | ❌ | ❌ |

**Railway ist die beste Option für deine Anforderungen!** 🎯
