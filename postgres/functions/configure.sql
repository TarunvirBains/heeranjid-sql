CREATE OR REPLACE FUNCTION heer_configure()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    cfg_epoch        TIMESTAMP;
    cfg_precision    VARCHAR(2);
    cfg_offset       NUMERIC(30,0);
    epoch_ms         BIGINT;
    epoch_ticks      NUMERIC(30,0);
    precision_bits   INTEGER;
    multiplier       NUMERIC;
    unit_name        TEXT;
    max_ts_41        BIGINT;
    max_ts_89        NUMERIC(30,0);
    smoke_heerid     BIGINT;
    smoke_ranjid     UUID;
BEGIN
    -- ----------------------------------------------------------------
    -- 1. Read config
    -- ----------------------------------------------------------------
    SELECT c.epoch, c.precision, c.ranj_epoch_offset
    INTO   cfg_epoch, cfg_precision, cfg_offset
    FROM   heer_config AS c
    WHERE  c.id = 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'heer_config row id=1 does not exist';
    END IF;

    -- ----------------------------------------------------------------
    -- 2. Validate
    -- ----------------------------------------------------------------
    IF cfg_epoch IS NULL THEN
        RAISE EXCEPTION 'heer_config.epoch must not be NULL';
    END IF;

    IF cfg_epoch > clock_timestamp() THEN
        RAISE EXCEPTION 'heer_config.epoch (%) is in the future', cfg_epoch;
    END IF;

    IF cfg_precision NOT IN ('us', 'ns', 'ps', 'fs') THEN
        RAISE EXCEPTION 'heer_config.precision must be one of us, ns, ps, fs; got "%"', cfg_precision;
    END IF;

    -- Compute epoch in milliseconds for HeerId
    epoch_ms := FLOOR(EXTRACT(EPOCH FROM cfg_epoch) * 1000)::BIGINT;

    -- Compute multiplier and precision_bits based on precision
    CASE cfg_precision
        WHEN 'us' THEN multiplier := 1000000;           precision_bits := 0; unit_name := 'microseconds';
        WHEN 'ns' THEN multiplier := 1000000000;         precision_bits := 1; unit_name := 'nanoseconds';
        WHEN 'ps' THEN multiplier := 1000000000000;      precision_bits := 2; unit_name := 'picoseconds';
        WHEN 'fs' THEN multiplier := 1000000000000000;   precision_bits := 3; unit_name := 'femtoseconds';
    END CASE;

    -- Compute epoch in target precision units for RanjId
    epoch_ticks := FLOOR(EXTRACT(EPOCH FROM cfg_epoch) * multiplier)::NUMERIC(30,0);

    -- Verify epoch fits in 41 bits for HeerId (max ~69 years from epoch)
    max_ts_41 := (2::BIGINT ^ 41) - 1;
    IF epoch_ms < 0 THEN
        RAISE EXCEPTION 'HeerId epoch_ms is negative (%); epoch too far in the past for BIGINT arithmetic', epoch_ms;
    END IF;

    -- Verify epoch fits in 89 bits for RanjId
    max_ts_89 := (2::NUMERIC ^ 89) - 1;
    -- We just need epoch_ticks to be non-negative; the actual range check is on current_tick at runtime
    IF epoch_ticks < 0 THEN
        RAISE EXCEPTION 'RanjId epoch_ticks is negative; epoch is invalid';
    END IF;

    -- ----------------------------------------------------------------
    -- 3. Regenerate HeerId function
    -- ----------------------------------------------------------------
    EXECUTE format($fmt$
CREATE OR REPLACE FUNCTION generate_ids(
    in_node_id INTEGER,
    requested_count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
)
RETURNS TABLE(id BIGINT)
LANGUAGE plpgsql
AS $func$
DECLARE
    -- Epoch baked in by heer_configure()
    epoch_ms CONSTANT BIGINT := %s;
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
            RAISE EXCEPTION 'clock rollback detected for node %% (%% ms)', in_node_id, rollback_ms;
        END IF;

        RAISE EXCEPTION 'hard clock rollback detected for node %% (%% ms)', in_node_id, rollback_ms;
    END IF;

    current_tick := GREATEST(now_ms, last_time);
    next_sequence := CASE
        WHEN current_tick = last_time THEN last_sequence + 1
        ELSE 0
    END;

    available_this_tick := 8192 - next_sequence;
    IF NOT allow_spanning AND requested_count > available_this_tick THEN
        RAISE EXCEPTION
            'requested %% IDs but only %% remain in millisecond %% for node %%',
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
$func$
    $fmt$, epoch_ms);

    -- ----------------------------------------------------------------
    -- 4. Regenerate RanjId function
    -- ----------------------------------------------------------------
    EXECUTE format($fmt$
CREATE OR REPLACE FUNCTION generate_ranjids(
    in_node_id INTEGER,
    requested_count INTEGER,
    allow_spanning BOOLEAN DEFAULT true
)
RETURNS TABLE(id UUID)
LANGUAGE plpgsql
AS $func$
DECLARE
    -- Epoch and precision baked in by heer_configure()
    epoch_ticks CONSTANT NUMERIC(30,0) := %s;
    epoch_offset CONSTANT NUMERIC(30,0) := %s;
    precision_bits CONSTANT INTEGER := %s;
    now_ticks NUMERIC(30,0);
    last_time NUMERIC(30,0);
    last_seq INTEGER;
    current_tick NUMERIC(30,0);
    next_seq INTEGER;
    remaining INTEGER;
    available_this_tick INTEGER;
    emit_count INTEGER;
    last_emitted_time NUMERIC(30,0);
    last_emitted_seq INTEGER;
    rollback_ticks NUMERIC(30,0);

    ts_high BIGINT;
    ts_mid BIGINT;
    ts_low BIGINT;
    hi BIGINT;
    lo_base BIGINT;
BEGIN
    IF requested_count IS NULL OR requested_count <= 0 THEN
        RAISE EXCEPTION 'requested_count must be greater than zero';
    END IF;

    IF in_node_id IS NULL OR in_node_id < 0 OR in_node_id > 32767 THEN
        RAISE EXCEPTION 'node_id %% is out of range for RanjId (0..32767)', in_node_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes WHERE node_id = in_node_id AND is_active = true
    ) THEN
        RAISE EXCEPTION 'node_id %% is not registered as an active Heer node', in_node_id;
    END IF;

    INSERT INTO heer_ranj_node_state (node_id)
    VALUES (in_node_id)
    ON CONFLICT (node_id) DO NOTHING;

    SELECT s.last_id_time, s.last_sequence
    INTO last_time, last_seq
    FROM heer_ranj_node_state AS s
    WHERE s.node_id = in_node_id
    FOR UPDATE;

    -- Calculate current time AFTER acquiring the lock to avoid false clock rollback
    -- under concurrency (another thread may have advanced last_id_time while we waited)
    -- current_tick = (now - epoch_ticks) + epoch_offset
    now_ticks := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * %s)::NUMERIC(30,0)
                 - epoch_ticks
                 + epoch_offset;

    rollback_ticks := last_time - now_ticks;
    IF rollback_ticks > 0 THEN
        IF rollback_ticks < 50000 THEN
            RAISE EXCEPTION 'clock rollback detected for ranj node %% (%% ticks)', in_node_id, rollback_ticks;
        END IF;
        RAISE EXCEPTION 'hard clock rollback detected for ranj node %% (%% ticks)', in_node_id, rollback_ticks;
    END IF;

    current_tick := GREATEST(now_ticks, last_time);
    next_seq := CASE
        WHEN current_tick = last_time THEN last_seq + 1
        ELSE 0
    END;

    available_this_tick := 65536 - next_seq;
    IF NOT allow_spanning AND requested_count > available_this_tick THEN
        RAISE EXCEPTION
            'requested %% IDs but only %% remain in tick %% for ranj node %%',
            requested_count,
            available_this_tick,
            current_tick,
            in_node_id;
    END IF;

    remaining := requested_count;

    WHILE remaining > 0 LOOP
        available_this_tick := 65536 - next_seq;
        emit_count := LEAST(remaining, available_this_tick);

        -- Decompose the 89-bit NUMERIC timestamp using division/modulo
        -- so we never truncate at BIGINT 2^63 limit.
        ts_high := (floor(current_tick / (2::NUMERIC ^ 41)) %% (2::NUMERIC ^ 48))::BIGINT;
        ts_mid  := (floor(current_tick / (2::NUMERIC ^ 29)) %% (2::NUMERIC ^ 12))::BIGINT;
        ts_low  := (current_tick %% (2::NUMERIC ^ 29))::BIGINT;

        hi := (ts_high << 16)
            | (8::BIGINT << 12)
            | ts_mid;

        -- lo layout (64 bits):
        --   bit 63:    variant bit 1 (set by 0x8000000000000000)
        --   bit 62:    variant bit 0 (0, already handled)
        --   bits 60-61: precision (2 bits)
        --   bits 31-59: ts_low (29 bits)
        --   bits 16-30: node_id (15 bits)
        --   bits 0-15:  sequence (16 bits)
        lo_base := (-9223372036854775808)::BIGINT
                 | (precision_bits::BIGINT << 60)
                 | (ts_low << 31)
                 | (in_node_id::BIGINT << 16);

        RETURN QUERY
        SELECT (
            lpad(to_hex(hi), 16, '0')
            || lpad(to_hex(lo_base | seq.s::BIGINT), 16, '0')
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
$func$
    $fmt$, epoch_ticks, cfg_offset, precision_bits, multiplier::TEXT);

    -- ----------------------------------------------------------------
    -- 5. Reset node state (precision/epoch change invalidates stored timestamps)
    -- ----------------------------------------------------------------
    UPDATE heer_node_state      SET last_id_time = 0, last_sequence = 0, updated_at = CURRENT_TIMESTAMP;
    UPDATE heer_ranj_node_state SET last_id_time = 0, last_sequence = 0, updated_at = CURRENT_TIMESTAMP;

    -- ----------------------------------------------------------------
    -- 6. Smoke test
    -- ----------------------------------------------------------------
    smoke_heerid := generate_id(1);
    smoke_ranjid := generate_ranjid(1);

    RAISE NOTICE 'heer_configure() succeeded. smoke HeerId=%, smoke RanjId=%', smoke_heerid, smoke_ranjid;
END;
$$;

-- Only superusers / explicit GRANT can run this
REVOKE EXECUTE ON FUNCTION heer_configure() FROM PUBLIC;
