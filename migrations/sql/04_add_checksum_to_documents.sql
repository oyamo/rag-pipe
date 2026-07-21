-- Liquibase formatted SQL
-- changeset oyamo:4-add-checksum-to-documents
ALTER TABLE documents ADD COLUMN IF NOT EXISTS checksum VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_documents_checksum ON documents(checksum);
