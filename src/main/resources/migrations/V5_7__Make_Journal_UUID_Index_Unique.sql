DROP INDEX journal_uuid_idx;
CREATE UNIQUE INDEX journal_uuid_idx ON journal(uuid);