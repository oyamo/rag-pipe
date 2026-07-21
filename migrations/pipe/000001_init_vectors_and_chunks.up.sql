CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS vectors (
    id UUID PRIMARY KEY,
    hash VARCHAR(64) NOT NULL UNIQUE,
    embedding vector(1536) NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    dimensions INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vectors_hash ON vectors(hash);
CREATE INDEX IF NOT EXISTS idx_vectors_model_version ON vectors(model_version);

CREATE TABLE IF NOT EXISTS chunks (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL,
    vector_id UUID NOT NULL REFERENCES vectors(id) ON DELETE CASCADE,
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT idx_chunks_doc_chunk_idx UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_chunks_metadata_gin ON chunks USING GIN (metadata);
