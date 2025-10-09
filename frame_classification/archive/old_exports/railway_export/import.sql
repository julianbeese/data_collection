
-- Schneller PostgreSQL Import
-- 1. Lade CSV-Dateien zu Railway hoch
-- 2. Führe dieses Script aus

-- Deaktiviere Autovacuum während Import
ALTER TABLE chunks SET (autovacuum_enabled = false);
ALTER TABLE agreement_chunks SET (autovacuum_enabled = false);

-- Import mit COPY (schnellste Methode)
\COPY chunks FROM 'chunks.csv' WITH CSV HEADER;
\COPY agreement_chunks FROM 'agreement_chunks.csv' WITH CSV HEADER;

-- Erstelle Indizes NACH Import
CREATE INDEX CONCURRENTLY idx_chunks_assigned_user ON chunks(assigned_user);
CREATE INDEX CONCURRENTLY idx_chunks_frame_label ON chunks(frame_label);
CREATE INDEX CONCURRENTLY idx_chunks_speaker ON chunks(speaker_name);
CREATE INDEX CONCURRENTLY idx_chunks_debate ON chunks(debate_id);

-- Aktiviere Autovacuum wieder
ALTER TABLE chunks SET (autovacuum_enabled = true);
ALTER TABLE agreement_chunks SET (autovacuum_enabled = true);

-- Analysiere für Statistiken
ANALYZE chunks;
ANALYZE agreement_chunks;
        