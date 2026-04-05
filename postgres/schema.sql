CREATE TABLE IF NOT EXISTS heer_nodes (
    node_id       INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT,
    is_active     BOOLEAN DEFAULT true,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS heer_config (
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    epoch               TIMESTAMP NOT NULL,
    precision           VARCHAR(2) NOT NULL DEFAULT 'ns',
    ranj_epoch_offset   NUMERIC(30,0) NOT NULL DEFAULT 0,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN heer_config.ranj_epoch_offset IS
'Extra time units added to the RanjId timestamp to represent epochs beyond '
'the range of TIMESTAMP (e.g. the Big Bang). The unit matches the RanjId '
'precision — microseconds by default. When 0, the epoch TIMESTAMP is used '
'directly. When set, current_tick = (now - epoch) + ranj_epoch_offset.';

CREATE TABLE IF NOT EXISTS heer_node_state (
    node_id         INTEGER PRIMARY KEY
                    REFERENCES heer_nodes(node_id) ON DELETE CASCADE,
    last_id_time    BIGINT NOT NULL DEFAULT 0,
    last_sequence   SMALLINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE heer_node_state IS
'Internal state for HeerId generator (one row per node). Do not modify manually.';

CREATE TABLE IF NOT EXISTS heer_ranj_node_state (
    node_id         INTEGER PRIMARY KEY
                    REFERENCES heer_nodes(node_id) ON DELETE CASCADE,
    last_id_time    NUMERIC(30,0) NOT NULL DEFAULT 0,
    last_sequence   INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
