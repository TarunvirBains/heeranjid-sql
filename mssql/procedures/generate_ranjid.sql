CREATE OR ALTER PROCEDURE generate_ranjids
    @in_node_id      INT = NULL,
    @requested_count INT,
    @allow_spanning  BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    IF @requested_count IS NULL OR @requested_count <= 0
        THROW 50200, 'requested_count must be greater than zero', 1;

    -- Resolve node_id from session context if not provided
    IF @in_node_id IS NULL
    BEGIN
        SET @in_node_id = dbo.heer_current_ranj_node_id();
        IF @in_node_id IS NULL
            THROW 50201, 'heer_ranj_node_id is not set for this session', 1;
    END

    -- Validate node
    IF @in_node_id < 0 OR @in_node_id > 32767
    BEGIN
        DECLARE @range_msg NVARCHAR(200) = CONCAT('node_id ', @in_node_id, ' is out of range for RanjId (0..32767)');
        THROW 50202, @range_msg, 1;
    END

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes
        WHERE node_id = @in_node_id AND is_active = 1
    )
    BEGIN
        DECLARE @active_msg NVARCHAR(200) = CONCAT('node_id ', @in_node_id, ' is not registered as an active Heer node');
        THROW 50203, @active_msg, 1;
    END

    -- Read epoch and offset
    DECLARE @epoch_us NUMERIC(38,0);
    DECLARE @epoch_offset NUMERIC(38,0);

    SELECT @epoch_us = DATEDIFF_BIG(MICROSECOND, '1970-01-01T00:00:00', epoch),
           @epoch_offset = ranj_epoch_offset
    FROM heer_config
    WHERE id = 1;

    IF @epoch_us IS NULL
        THROW 50204, 'heer_config row id=1 must exist before generating IDs', 1;

    -- Temp table for results (created outside transaction)
    CREATE TABLE #ranj_ids (id BINARY(16));

    BEGIN TRANSACTION;

    -- Ensure state row exists
    IF NOT EXISTS (SELECT 1 FROM heer_ranj_node_state WHERE node_id = @in_node_id)
        INSERT INTO heer_ranj_node_state (node_id) VALUES (@in_node_id);

    -- Lock and read state
    DECLARE @last_time NUMERIC(38,0);
    DECLARE @last_seq INT;

    SELECT @last_time = last_id_time,
           @last_seq = last_sequence
    FROM heer_ranj_node_state WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
    WHERE node_id = @in_node_id;

    -- Calculate current time AFTER acquiring the lock to avoid false clock rollback
    -- under concurrency (another thread may have advanced last_id_time while we waited)
    DECLARE @now_us NUMERIC(38,0) = CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01T00:00:00', SYSUTCDATETIME()) AS NUMERIC(38,0))
                                    - @epoch_us
                                    + @epoch_offset;

    -- Clock rollback detection (50000 microsecond threshold)
    DECLARE @rollback_us NUMERIC(38,0) = @last_time - @now_us;
    IF @rollback_us > 0
    BEGIN
        IF @rollback_us < 50000
        BEGIN
            DECLARE @soft_msg NVARCHAR(200) = CONCAT('clock rollback detected for ranj node ', @in_node_id, ' (', CAST(@rollback_us AS NVARCHAR(40)), ' us)');
            ROLLBACK TRANSACTION;
            THROW 50021, @soft_msg, 1;
        END
        ELSE
        BEGIN
            DECLARE @hard_msg NVARCHAR(200) = CONCAT('hard clock rollback detected for ranj node ', @in_node_id, ' (', CAST(@rollback_us AS NVARCHAR(40)), ' us)');
            ROLLBACK TRANSACTION;
            THROW 50023, @hard_msg, 1;
        END
    END

    -- Determine starting tick and sequence
    DECLARE @current_tick NUMERIC(38,0) = CASE WHEN @now_us > @last_time THEN @now_us ELSE @last_time END;
    DECLARE @next_seq INT = CASE WHEN @current_tick = @last_time THEN @last_seq + 1 ELSE 0 END;

    -- Check capacity
    DECLARE @available_this_tick INT = 65536 - @next_seq;
    IF @allow_spanning = 0 AND @requested_count > @available_this_tick
    BEGIN
        DECLARE @cap_msg NVARCHAR(400) = CONCAT(
            'requested ', @requested_count,
            ' IDs but only ', @available_this_tick,
            ' remain in microsecond ', CAST(@current_tick AS NVARCHAR(40)),
            ' for ranj node ', @in_node_id
        );
        ROLLBACK TRANSACTION;
        THROW 50205, @cap_msg, 1;
    END

    -- Constants for NUMERIC(38,0) power-of-2 arithmetic
    DECLARE @two NUMERIC(38,0) = 2;

    DECLARE @remaining INT = @requested_count;
    DECLARE @emit_count INT;
    DECLARE @last_emitted_time NUMERIC(38,0);
    DECLARE @last_emitted_seq INT;
    DECLARE @seq INT;

    -- Pre-compute powers used in the loop
    DECLARE @pow2_41 NUMERIC(38,0) = POWER(@two, 41);
    DECLARE @pow2_48 NUMERIC(38,0) = POWER(@two, 48);
    DECLARE @pow2_29 NUMERIC(38,0) = POWER(@two, 29);
    DECLARE @pow2_12 NUMERIC(38,0) = POWER(@two, 12);
    DECLARE @precision_bits BIGINT = 1; -- nanoseconds (default)

    -- Variables used inside the loop (declared once, assigned per iteration)
    DECLARE @ts_high BIGINT;
    DECLARE @ts_mid  BIGINT;
    DECLARE @ts_low  BIGINT;
    DECLARE @hi      BIGINT;
    DECLARE @lo_base BIGINT;
    DECLARE @lo      BIGINT;
    DECLARE @hi_bytes BINARY(8);

    WHILE @remaining > 0
    BEGIN
        SET @available_this_tick = 65536 - @next_seq;
        SET @emit_count = CASE WHEN @remaining < @available_this_tick THEN @remaining ELSE @available_this_tick END;

        -- Decompose 89-bit timestamp using NUMERIC(38,0) arithmetic
        SET @ts_high = CAST(FLOOR(@current_tick / @pow2_41) % @pow2_48 AS BIGINT);
        SET @ts_mid  = CAST(FLOOR(@current_tick / @pow2_29) % @pow2_12 AS BIGINT);
        SET @ts_low  = CAST(@current_tick % @pow2_29 AS BIGINT);

        -- Upper 8 bytes: ts_high(48) | version=8(4) | ts_mid(12)
        SET @hi = (@ts_high * POWER(CAST(2 AS BIGINT), 16))
                | (CAST(8 AS BIGINT) * POWER(CAST(2 AS BIGINT), 12))
                | @ts_mid;

        -- Pre-compute the fixed portion of the lower 8 bytes (everything except sequence)
        SET @lo_base = CAST(0x8000000000000000 AS BIGINT)  -- variant bits 10
                     | (@precision_bits * POWER(CAST(2 AS BIGINT), 60))
                     | (@ts_low * POWER(CAST(2 AS BIGINT), 31))
                     | (CAST(@in_node_id AS BIGINT) * POWER(CAST(2 AS BIGINT), 16));

        SET @hi_bytes = CAST(@hi AS BINARY(8));

        SET @seq = @next_seq;
        WHILE @seq < @next_seq + @emit_count
        BEGIN
            SET @lo = @lo_base | CAST(@seq AS BIGINT);

            INSERT INTO #ranj_ids (id)
            VALUES (@hi_bytes + CAST(@lo AS BINARY(8)));

            SET @seq = @seq + 1;
        END

        SET @last_emitted_time = @current_tick;
        SET @last_emitted_seq = @next_seq + @emit_count - 1;
        SET @remaining = @remaining - @emit_count;
        SET @current_tick = @current_tick + 1;
        SET @next_seq = 0;
    END

    -- Update state
    UPDATE heer_ranj_node_state
    SET last_id_time = @last_emitted_time,
        last_sequence = @last_emitted_seq,
        updated_at = SYSUTCDATETIME()
    WHERE node_id = @in_node_id;

    COMMIT TRANSACTION;

    -- Return results
    SELECT id FROM #ranj_ids;

    DROP TABLE #ranj_ids;
END
GO

CREATE OR ALTER PROCEDURE generate_ranjid
    @in_node_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC generate_ranjids @in_node_id = @in_node_id, @requested_count = 1, @allow_spanning = 1;
END
GO
