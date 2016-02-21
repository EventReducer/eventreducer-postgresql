ALTER TABLE journal ADD COLUMN hash BYTEA;

UPDATE journal SET hash = decode(event->>'@hash', 'base64');