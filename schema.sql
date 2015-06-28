
CREATE TABLE messages(
    uuid TEXT NOT NULL PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    format TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload TEXT
);

CREATE INDEX messages_index ON messages (uuid, timestamp, topic);

