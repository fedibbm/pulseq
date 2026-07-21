-- PulseQ PostgreSQL Schema
CREATE TABLE IF NOT EXISTS messages (
    id                  VARCHAR(36)  PRIMARY KEY,
    topic               VARCHAR(255) NOT NULL,
    payload             BYTEA        NOT NULL,
    published_at        BIGINT       NOT NULL,
    status              VARCHAR(16)  NOT NULL DEFAULT 'AVAILABLE',
    delivery_attempts   INT          NOT NULL DEFAULT 0,
    visibility_expires_at BIGINT    NOT NULL DEFAULT 0,
    max_retries         INT          NOT NULL DEFAULT 3
);

CREATE INDEX IF NOT EXISTS idx_messages_topic_status ON messages(topic, status);
