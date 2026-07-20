CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(128),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    file_key VARCHAR(512) NOT NULL,
    file_size BIGINT NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'INGESTED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_tenant_id ON documents(tenant_id);
CREATE INDEX IF NOT EXISTS idx_documents_created_at ON documents(created_at);
