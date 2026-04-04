CREATE OR REPLACE FUNCTION generate_ranjids(
    in_node_id INTEGER,
    requested_count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
)
RETURNS TABLE(id UUID)
LANGUAGE plpgsql
AS $$
DECLARE
    epoch_us NUMERIC(30,0);
    now_us NUMERIC(30,0);
    last_time NUMERIC(30,0);
    last_seq INTEGER;
    current_tick NUMERIC(30,0);
    next_seq INTEGER;
    remaining INTEGER;
    available_this_tick INTEGER;
    emit_count INTEGER;
    last_emitted_time NUMERIC(30,0);
    last_emitted_seq INTEGER;
    rollback_us NUMERIC(30,0);

    ts_high BIGINT;
    ts_mid BIGINT;
    ts_low BIGINT;
    hi BIGINT;
    lo BIGINT;
BEGIN
    IF requested_count IS NULL OR requested_count <= 0 THEN
        RAISE EXCEPTION 'requested_count must be greater than zero';
    END IF;

    IF in_node_id IS NULL OR in_node_id < 0 OR in_node_id > 65535 THEN
        RAISE EXCEPTION 'node_id % is out of range for RanjId (0..65535)', in_node_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes WHERE node_id = in_node_id AND is_active = true
    ) THEN
        RAISE EXCEPTION 'node_id % is not registered as an active Heer node', in_node_id;
    END IF;

    SELECT FLOOR(EXTRACT(EPOCH FROM c.epoch) * 1000000)::NUMERIC(30,0)
    INTO epoch_us
    FROM heer_config AS c
    WHERE c.id = 1;

    IF epoch_us IS NULL THEN
        RAISE EXCEPTION 'heer_config row id=1 must exist before generating IDs';
    END IF;

    now_us := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::NUMERIC(30,0) - epoch_us;

    INSERT INTO heer_ranj_node_state (node_id)
    VALUES (in_node_id)
    ON CONFLICT (node_id) DO NOTHING;

    SELECT s.last_id_time, s.last_sequence
    INTO last_time, last_seq
    FROM heer_ranj_node_state AS s
    WHERE s.node_id = in_node_id
    FOR UPDATE;

    rollback_us := last_time - now_us;
    IF rollback_us > 0 THEN
        IF rollback_us < 50000 THEN
            RAISE EXCEPTION 'clock rollback detected for ranj node % (% us)', in_node_id, rollback_us;
        END IF;
        RAISE EXCEPTION 'hard clock rollback detected for ranj node % (% us)', in_node_id, rollback_us;
    END IF;

    current_tick := GREATEST(now_us, last_time);
    next_seq := CASE
        WHEN current_tick = last_time THEN last_seq + 1
        ELSE 0
    END;

    available_this_tick := 65536 - next_seq;
    IF NOT allow_spanning AND requested_count > available_this_tick THEN
        RAISE EXCEPTION
            'requested % IDs but only % remain in microsecond % for ranj node %',
            requested_count,
            available_this_tick,
            current_tick,
            in_node_id;
    END IF;

    remaining := requested_count;

    WHILE remaining > 0 LOOP
        available_this_tick := 65536 - next_seq;
        emit_count := LEAST(remaining, available_this_tick);

        ts_high := (current_tick >> 42)::BIGINT & ((1::BIGINT << 48) - 1);
        ts_mid  := (current_tick >> 30)::BIGINT & ((1::BIGINT << 12) - 1);
        ts_low  := current_tick::BIGINT & ((1::BIGINT << 30) - 1);

        hi := (ts_high << 16)
            | (7::BIGINT << 12)
            | ts_mid;

        RETURN QUERY
        SELECT (
            lpad(to_hex(hi), 16, '0')
            || lpad(to_hex(
                ((-9223372036854775808)::BIGINT  -- 0x8000000000000000 (sets variant bit 1)
                | (ts_low << 32)
                | (in_node_id::BIGINT << 16)
                | seq.s::BIGINT)
            ), 16, '0')
        )::UUID AS id
        FROM generate_series(next_seq, next_seq + emit_count - 1) AS seq(s);

        last_emitted_time := current_tick;
        last_emitted_seq := next_seq + emit_count - 1;
        remaining := remaining - emit_count;
        current_tick := current_tick + 1;
        next_seq := 0;
    END LOOP;

    UPDATE heer_ranj_node_state
    SET last_id_time = last_emitted_time,
        last_sequence = last_emitted_seq,
        updated_at = CURRENT_TIMESTAMP
    WHERE node_id = in_node_id;
END;
$$;

CREATE OR REPLACE FUNCTION generate_ranjids(
    requested_count INTEGER,
    allow_spanning BOOLEAN
)
RETURNS TABLE(id UUID)
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ranjids(current_heer_node_id(), requested_count, allow_spanning);
$$;

CREATE OR REPLACE FUNCTION generate_ranjids(requested_count INTEGER)
RETURNS TABLE(id UUID)
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ranjids(current_heer_node_id(), requested_count, true);
$$;

CREATE OR REPLACE FUNCTION generate_ranjid(in_node_id INTEGER)
RETURNS UUID
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ranjids(in_node_id, 1, true);
$$;

CREATE OR REPLACE FUNCTION generate_ranjid()
RETURNS UUID
LANGUAGE sql
AS $$
    SELECT id
    FROM generate_ranjids(current_heer_node_id(), 1, true);
$$;
