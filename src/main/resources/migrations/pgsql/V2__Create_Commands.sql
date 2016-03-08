CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE commands (
  uuid UUID UNIQUE,
  command JSONB
);

ALTER TABLE journal ADD COLUMN command UUID;

UPDATE journal SET command = uuid_generate_v4();

INSERT INTO commands (uuid, command) (SELECT command AS uuid, event->'command' AS command FROM journal);

UPDATE journal SET event = event - 'command';

ALTER TABLE journal ADD FOREIGN KEY (command) REFERENCES commands(uuid);