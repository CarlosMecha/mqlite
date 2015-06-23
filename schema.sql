
CREATE TABLE notifications(
    uuid TEXT NOT NULL PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    format TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload TEXT
);

CREATE INDEX notifications_index ON notifications (uuid, timestamp, topic);

