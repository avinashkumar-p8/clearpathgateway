-- Migration script for creating message_unique_ids table
-- This table stores MUIDs for idempotent message processing

-- Create the main table
CREATE TABLE message_unique_ids (
    id INT64 NOT NULL,
    muid STRING(255) NOT NULL,
    topic STRING(100) NOT NULL,
    partition INT64 NOT NULL,
    offset INT64 NOT NULL,
    event_payload STRING(MAX),
    processing_status STRING(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    is_active BOOL NOT NULL
) PRIMARY KEY (id);

-- Create unique index on MUID
CREATE UNIQUE INDEX idx_message_unique_ids_muid ON message_unique_ids (muid);

-- Create composite index for topic/partition lookups
CREATE INDEX idx_message_unique_ids_topic_partition ON message_unique_ids (topic, partition);

-- Create index for processing status queries
CREATE INDEX idx_message_unique_ids_status ON message_unique_ids (processing_status, is_active);

-- Create index for timestamp-based queries
CREATE INDEX idx_message_unique_ids_created ON message_unique_ids (created_at, is_active);

-- Insert initial configuration record (optional)
INSERT INTO message_unique_ids (
    id, muid, topic, partition, offset, event_payload, 
    processing_status, created_at, is_active
) VALUES (
    1, 'INIT-SETUP', 'system', 0, 0, '{"type": "system_initialization"}',
    'COMPLETED', CURRENT_TIMESTAMP(), true
);

