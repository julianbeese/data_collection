
-- ULTRA-SCHNELLER IMPORT für Railway
-- Nutze diese SQL-Datei für schnellen Import

-- 1. Lade CSV-Dateien zu Railway hoch
-- 2. Führe diese SQL-Befehle aus:

\copy chunks FROM 'railway_export_optimized/chunks.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"');

-- Falls Agreement-Chunks vorhanden:
\copy agreement_chunks FROM 'railway_export_optimized/agreement_chunks.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"');

-- Erstelle Indizes für Performance:
CREATE INDEX IF NOT EXISTS idx_chunks_speech_id ON chunks(speech_id);
CREATE INDEX IF NOT EXISTS idx_chunks_assigned_user ON chunks(assigned_user);
CREATE INDEX IF NOT EXISTS idx_chunks_frame_label ON chunks(frame_label);
CREATE INDEX IF NOT EXISTS idx_agreement_annotator1 ON agreement_chunks(annotator1);
CREATE INDEX IF NOT EXISTS idx_agreement_annotator2 ON agreement_chunks(annotator2);
