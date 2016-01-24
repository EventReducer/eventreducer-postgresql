CREATE INDEX journal_class_idx ON journal((event->>'@class'));
CREATE INDEX command_class_idx ON commands((command->>'@class'));