CREATE OR REPLACE FUNCTION generate_ids(
    in_node_id INTEGER,
    requested_count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
)
RETURNS TABLE(id BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    epoch_ms BIGINT;
    now_ms BIGINT;
    last_time BIGINT;
    last_sequence INTEGER;
    current_tick BIGINT;
    next_sequence INTEGER;
    remaining INTEGER;
    available_this_tick INTEGER;
    emit_count INTEGER;
    last_emitted_time BIGINT;
    last_emitted_sequence INTEGER;
    rollback_ms BIGINT;
BEGIN
    IF requested_count IS NULL OR requested_count <= 0 THEN
        RAISE EXCEPTION 'requested_count must be greater than zero';
    END IF;

    PERFORM set_heer_node_id(in_node_id);

    SELECT FLOOR(EXTRACT(EPOCH FROM c.epoch) * 1000)::BIGINT
    INTO epoch_ms
    FROM heer_config AS c
    WHERE c.id = 1;

    IF epoch_ms IS NULL THEN
        RAISE EXCEPTION 'heer_config row id=1 must exist before generating IDs';
    END IF;

    INSERT INTO heer_node_state (node_id)
    VALUES (in_node_id)
    ON CONFLICT (node_id) DO NOTHING;

    SELECT s.last_id_time, s.last_sequence
    INTO last_time, last_sequence
    FROM heer_node_state AS s
    WHERE s.node_id = in_node_id
    FOR UPDATE;

    -- Calculate current time AFTER acquiring the lock to avoid false clock rollback
    -- under concurrency (another thread may have advanced last_id_time while we waited)
    now_ms := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - epoch_ms;

    rollback_ms := last_time - now_ms;
    IF rollback_ms > 0 THEN
        IF rollback_ms < 50 THEN
            RAISE EXCEPTION 'clock rollback detected for node % (% ms)', in_node_id, rollback_ms;
        END IF;

        RAISE EXCEPTION 'hard clock rollback detected for node % (% ms)', in_node_id, rollback_ms;
    END IF;

    current_tick := GREATEST(now_ms, last_time);
    next_sequence := CASE
        WHEN current_tick = last_time THEN last_sequence + 1
        ELSE 0
    END;

    available_this_tick := 8192 - next_sequence;
    IF NOT allow_spanning AND requested_count > available_this_tick THEN
        RAISE EXCEPTION
            'requested % IDs but only % remain in millisecond % for node %',
            requested_count,
            available_this_tick,
            current_tick,
            in_node_id;
    END IF;

    remaining := requested_count;

    WHILE remaining > 0 LOOP
        available_this_tick := 8192 - next_sequence;
        emit_count := LEAST(remaining, available_this_tick);

        RETURN QUERY
        SELECT
            (
                (current_tick::BIGINT << 22)
                | (in_node_id::BIGINT << 13)
                | series.sequence::BIGINT
            ) AS id
        FROM generate_series(next_sequence, next_sequence + emit_count - 1) AS series(sequence);

        last_emitted_time := current_tick;
        last_emitted_sequence := next_sequence + emit_count - 1;
        remaining := remaining - emit_count;
        current_tick := current_tick + 1;
        next_sequence := 0;
    END LOOP;

    UPDATE heer_node_state
    SET last_id_time = last_emitted_time,
        last_sequence = last_emitted_sequence,
        updated_at = CURRENT_TIMESTAMP
    WHERE node_id = in_node_id;
END;
$$;

CREATE OR REPLACE FUNCTION generate_ids(
    requested_count INTEGER,
    allow_spanning BOOLEAN
)
RETURNS TABLE(id BIGINT)
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ids(current_heer_node_id(), requested_count, allow_spanning);
$$;

CREATE OR REPLACE FUNCTION generate_ids(requested_count INTEGER)
RETURNS TABLE(id BIGINT)
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ids(current_heer_node_id(), requested_count, true);
$$;

CREATE OR REPLACE FUNCTION generate_id(in_node_id INTEGER)
RETURNS BIGINT
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ids(in_node_id, 1, true);
$$;

CREATE OR REPLACE FUNCTION generate_id()
RETURNS BIGINT
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ids(current_heer_node_id(), 1, true);
$$;
