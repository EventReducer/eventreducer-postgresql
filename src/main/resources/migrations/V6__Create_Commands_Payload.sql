ALTER TABLE commands ADD COLUMN payload BYTEA;
ALTER TABLE commands ADD COLUMN hash BYTEA;
ALTER TABLE commands ADD COLUMN created_at BIGINT;

CREATE INDEX commands_hash_idx ON commands(hash);

UPDATE commands SET hash = decode(command->>'@hash', 'base64');