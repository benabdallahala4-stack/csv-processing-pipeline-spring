CREATE TABLE csv_upload (
 id uuid PRIMARY KEY, file_name varchar(255) NOT NULL, content_type varchar(100) NOT NULL,
 size_bytes bigint NOT NULL CHECK (size_bytes > 0), sha256 varchar(64) NOT NULL, object_key varchar(500) NOT NULL UNIQUE,
 status varchar(30) NOT NULL, attempt_count integer NOT NULL DEFAULT 0, third_party_reference varchar(255), failure_code varchar(100),
 next_attempt_at timestamptz, lease_owner varchar(100), lease_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE http_idempotency (operation varchar(80) NOT NULL, idempotency_key varchar(200) NOT NULL, request_hash varchar(64) NOT NULL, upload_id uuid NOT NULL REFERENCES csv_upload(id), created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(operation,idempotency_key));
CREATE TABLE outbox_event (id uuid PRIMARY KEY, aggregate_id uuid NOT NULL REFERENCES csv_upload(id), event_type varchar(100) NOT NULL, payload jsonb NOT NULL, attempts integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(), lease_owner varchar(100), lease_until timestamptz, published_at timestamptz, last_error text, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(aggregate_id,event_type));
CREATE TABLE consumer_inbox (consumer_name varchar(100) NOT NULL, event_id uuid NOT NULL, state varchar(30) NOT NULL, attempts integer NOT NULL DEFAULT 0, lease_owner varchar(100), lease_until timestamptz, completed_at timestamptz, PRIMARY KEY(consumer_name,event_id));
CREATE INDEX idx_outbox_ready ON outbox_event(available_at,created_at) WHERE published_at IS NULL;
CREATE INDEX idx_upload_retry ON csv_upload(status,next_attempt_at) WHERE status='RETRY_PENDING';
CREATE INDEX idx_upload_created ON csv_upload(created_at DESC);
