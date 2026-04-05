CREATE OR ALTER PROCEDURE heer_configure
AS
BEGIN
    SET NOCOUNT ON;

    -- ----------------------------------------------------------------
    -- 1. Read config
    -- ----------------------------------------------------------------
    DECLARE @cfg_epoch DATETIME2;
    DECLARE @cfg_precision VARCHAR(2);
    DECLARE @cfg_offset NUMERIC(38,0);

    SELECT @cfg_epoch = epoch,
           @cfg_precision = precision,
           @cfg_offset = ranj_epoch_offset
    FROM heer_config
    WHERE id = 1;

    IF @cfg_epoch IS NULL
        THROW 50300, 'heer_config row id=1 does not exist or epoch is NULL', 1;

    -- ----------------------------------------------------------------
    -- 2. Validate
    -- ----------------------------------------------------------------
    IF @cfg_epoch > SYSUTCDATETIME()
    BEGIN
        DECLARE @future_msg NVARCHAR(400) = CONCAT('heer_config.epoch (', CONVERT(NVARCHAR(30), @cfg_epoch, 126), ') is in the future');
        THROW 50301, @future_msg, 1;
    END

    IF @cfg_precision NOT IN ('us', 'ns', 'ps', 'fs')
    BEGIN
        DECLARE @prec_msg NVARCHAR(400) = CONCAT('heer_config.precision must be one of us, ns, ps, fs; got "', @cfg_precision, '"');
        THROW 50302, @prec_msg, 1;
    END

    -- Compute epoch in milliseconds for HeerId
    DECLARE @epoch_ms BIGINT = DATEDIFF_BIG(MILLISECOND, '1970-01-01T00:00:00', @cfg_epoch);

    -- Compute multiplier, precision_bits, and unit_name based on precision
    DECLARE @multiplier NVARCHAR(20);
    DECLARE @precision_bits INT;
    DECLARE @unit_name NVARCHAR(20);
    DECLARE @datediff_unit NVARCHAR(20);

    IF @cfg_precision = 'us'
    BEGIN
        SET @multiplier = '1000000';
        SET @precision_bits = 0;
        SET @unit_name = 'microseconds';
        SET @datediff_unit = 'MICROSECOND';
    END
    ELSE IF @cfg_precision = 'ns'
    BEGIN
        SET @multiplier = '1000000000';
        SET @precision_bits = 1;
        SET @unit_name = 'nanoseconds';
        SET @datediff_unit = 'NANOSECOND';
    END
    ELSE IF @cfg_precision = 'ps'
    BEGIN
        SET @multiplier = '1000000000000';
        SET @precision_bits = 2;
        SET @unit_name = 'picoseconds';
        SET @datediff_unit = 'NANOSECOND'; -- MSSQL max is nanosecond; multiply by 1000
    END
    ELSE IF @cfg_precision = 'fs'
    BEGIN
        SET @multiplier = '1000000000000000';
        SET @precision_bits = 3;
        SET @unit_name = 'femtoseconds';
        SET @datediff_unit = 'NANOSECOND'; -- MSSQL max is nanosecond; multiply by 1000000
    END

    -- Compute epoch_ticks: epoch expressed in the chosen precision unit
    -- MSSQL DATEDIFF_BIG maxes out at NANOSECOND, so for ps/fs we derive from nanoseconds
    DECLARE @epoch_ticks NUMERIC(38,0);

    IF @cfg_precision = 'us'
        SET @epoch_ticks = CAST(DATEDIFF_BIG(MICROSECOND, '1970-01-01T00:00:00', @cfg_epoch) AS NUMERIC(38,0));
    ELSE IF @cfg_precision = 'ns'
        SET @epoch_ticks = CAST(DATEDIFF_BIG(NANOSECOND, '1970-01-01T00:00:00', @cfg_epoch) AS NUMERIC(38,0));
    ELSE IF @cfg_precision = 'ps'
        SET @epoch_ticks = CAST(DATEDIFF_BIG(NANOSECOND, '1970-01-01T00:00:00', @cfg_epoch) AS NUMERIC(38,0)) * 1000;
    ELSE IF @cfg_precision = 'fs'
        SET @epoch_ticks = CAST(DATEDIFF_BIG(NANOSECOND, '1970-01-01T00:00:00', @cfg_epoch) AS NUMERIC(38,0)) * 1000000;

    IF @epoch_ticks < 0
        THROW 50303, 'RanjId epoch_ticks is negative; epoch is invalid', 1;

    -- ----------------------------------------------------------------
    -- 3. Begin transaction for all DDL + state reset + smoke test
    -- ----------------------------------------------------------------
    DECLARE @sql NVARCHAR(MAX);

    BEGIN TRANSACTION;

    BEGIN TRY

    -- ----------------------------------------------------------------
    -- 4. Regenerate generate_ids (HeerId) with baked-in epoch
    -- ----------------------------------------------------------------
    SET @sql = N'
CREATE OR ALTER PROCEDURE generate_ids
    @in_node_id      INT = NULL,
    @requested_count INT,
    @allow_spanning  BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    IF @requested_count IS NULL OR @requested_count <= 0
        THROW 50100, ''requested_count must be greater than zero'', 1;

    -- Resolve node_id from session context if not provided
    IF @in_node_id IS NULL
    BEGIN
        SET @in_node_id = dbo.heer_current_node_id();
        IF @in_node_id IS NULL
            THROW 50101, ''heer_node_id is not set for this session'', 1;
    END

    -- Validate node
    EXEC heer_set_node_id @in_node_id;

    -- Epoch baked in by heer_configure
    DECLARE @epoch_ms BIGINT = ' + CAST(@epoch_ms AS NVARCHAR(30)) + N';

    -- Temp table for results (created outside transaction)
    CREATE TABLE #heer_ids (id BIGINT);

    BEGIN TRANSACTION;

    -- Ensure state row exists
    IF NOT EXISTS (SELECT 1 FROM heer_node_state WHERE node_id = @in_node_id)
        INSERT INTO heer_node_state (node_id) VALUES (@in_node_id);

    -- Lock and read state
    DECLARE @last_time BIGINT;
    DECLARE @last_sequence INT;

    SELECT @last_time = last_id_time,
           @last_sequence = last_sequence
    FROM heer_node_state WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
    WHERE node_id = @in_node_id;

    DECLARE @now_ms BIGINT = DATEDIFF_BIG(MILLISECOND, ''1970-01-01T00:00:00'', SYSUTCDATETIME()) - @epoch_ms;

    -- Clock rollback detection
    DECLARE @rollback_ms BIGINT = @last_time - @now_ms;
    IF @rollback_ms > 0
    BEGIN
        IF @rollback_ms < 50
        BEGIN
            DECLARE @soft_msg NVARCHAR(200) = CONCAT(''clock rollback detected for node '', @in_node_id, '' ('', @rollback_ms, '' ms)'');
            ROLLBACK TRANSACTION;
            THROW 50020, @soft_msg, 1;
        END
        ELSE
        BEGIN
            DECLARE @hard_msg NVARCHAR(200) = CONCAT(''hard clock rollback detected for node '', @in_node_id, '' ('', @rollback_ms, '' ms)'');
            ROLLBACK TRANSACTION;
            THROW 50022, @hard_msg, 1;
        END
    END

    -- Determine starting tick and sequence
    DECLARE @current_tick BIGINT = CASE WHEN @now_ms > @last_time THEN @now_ms ELSE @last_time END;
    DECLARE @next_sequence INT = CASE WHEN @current_tick = @last_time THEN @last_sequence + 1 ELSE 0 END;

    -- Check capacity
    DECLARE @available_this_tick INT = 8192 - @next_sequence;
    IF @allow_spanning = 0 AND @requested_count > @available_this_tick
    BEGIN
        DECLARE @cap_msg NVARCHAR(400) = CONCAT(
            ''requested '', @requested_count,
            '' IDs but only '', @available_this_tick,
            '' remain in millisecond '', @current_tick,
            '' for node '', @in_node_id
        );
        ROLLBACK TRANSACTION;
        THROW 50103, @cap_msg, 1;
    END

    DECLARE @remaining INT = @requested_count;
    DECLARE @emit_count INT;
    DECLARE @last_emitted_time BIGINT;
    DECLARE @last_emitted_sequence INT;
    DECLARE @seq INT;
    DECLARE @node_shifted BIGINT = CAST(@in_node_id AS BIGINT) * POWER(CAST(2 AS BIGINT), 13);
    DECLARE @tick_shifted BIGINT;

    WHILE @remaining > 0
    BEGIN
        SET @available_this_tick = 8192 - @next_sequence;
        SET @emit_count = CASE WHEN @remaining < @available_this_tick THEN @remaining ELSE @available_this_tick END;

        SET @seq = @next_sequence;
        SET @tick_shifted = @current_tick * POWER(CAST(2 AS BIGINT), 22);

        WHILE @seq < @next_sequence + @emit_count
        BEGIN
            INSERT INTO #heer_ids (id)
            VALUES (@tick_shifted | @node_shifted | CAST(@seq AS BIGINT));

            SET @seq = @seq + 1;
        END

        SET @last_emitted_time = @current_tick;
        SET @last_emitted_sequence = @next_sequence + @emit_count - 1;
        SET @remaining = @remaining - @emit_count;
        SET @current_tick = @current_tick + 1;
        SET @next_sequence = 0;
    END

    -- Update state
    UPDATE heer_node_state
    SET last_id_time = @last_emitted_time,
        last_sequence = @last_emitted_sequence,
        updated_at = SYSUTCDATETIME()
    WHERE node_id = @in_node_id;

    COMMIT TRANSACTION;

    -- Return results
    SELECT id FROM #heer_ids;

    DROP TABLE #heer_ids;
END';

    EXEC sp_executesql @sql;

    -- ----------------------------------------------------------------
    -- 5. Regenerate generate_id (single-ID wrapper)
    -- ----------------------------------------------------------------
    SET @sql = N'
CREATE OR ALTER PROCEDURE generate_id
    @in_node_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC generate_ids @in_node_id = @in_node_id, @requested_count = 1, @allow_spanning = 1;
END';

    EXEC sp_executesql @sql;

    -- ----------------------------------------------------------------
    -- 6. Regenerate generate_ranjids with baked-in epoch, precision, offset
    -- ----------------------------------------------------------------

    -- Build the now_ticks calculation depending on precision
    -- MSSQL DATEDIFF_BIG supports up to NANOSECOND, so for ps/fs we multiply
    DECLARE @now_ticks_expr NVARCHAR(MAX);

    IF @cfg_precision = 'us'
        SET @now_ticks_expr = N'CAST(DATEDIFF_BIG(MICROSECOND, ''1970-01-01T00:00:00'', SYSUTCDATETIME()) AS NUMERIC(38,0))';
    ELSE IF @cfg_precision = 'ns'
        SET @now_ticks_expr = N'CAST(DATEDIFF_BIG(NANOSECOND, ''1970-01-01T00:00:00'', SYSUTCDATETIME()) AS NUMERIC(38,0))';
    ELSE IF @cfg_precision = 'ps'
        SET @now_ticks_expr = N'CAST(DATEDIFF_BIG(NANOSECOND, ''1970-01-01T00:00:00'', SYSUTCDATETIME()) AS NUMERIC(38,0)) * 1000';
    ELSE IF @cfg_precision = 'fs'
        SET @now_ticks_expr = N'CAST(DATEDIFF_BIG(NANOSECOND, ''1970-01-01T00:00:00'', SYSUTCDATETIME()) AS NUMERIC(38,0)) * 1000000';

    SET @sql = N'
CREATE OR ALTER PROCEDURE generate_ranjids
    @in_node_id      INT = NULL,
    @requested_count INT,
    @allow_spanning  BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    IF @requested_count IS NULL OR @requested_count <= 0
        THROW 50200, ''requested_count must be greater than zero'', 1;

    -- Resolve node_id from session context if not provided
    IF @in_node_id IS NULL
    BEGIN
        SET @in_node_id = dbo.heer_current_ranj_node_id();
        IF @in_node_id IS NULL
            THROW 50201, ''heer_ranj_node_id is not set for this session'', 1;
    END

    -- Validate node (15-bit node_id: 0..32767)
    IF @in_node_id < 0 OR @in_node_id > 32767
    BEGIN
        DECLARE @range_msg NVARCHAR(200) = CONCAT(''node_id '', @in_node_id, '' is out of range for RanjId (0..32767)'');
        THROW 50202, @range_msg, 1;
    END

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes
        WHERE node_id = @in_node_id AND is_active = 1
    )
    BEGIN
        DECLARE @active_msg NVARCHAR(200) = CONCAT(''node_id '', @in_node_id, '' is not registered as an active Heer node'');
        THROW 50203, @active_msg, 1;
    END

    -- Epoch and precision baked in by heer_configure
    DECLARE @epoch_ticks NUMERIC(38,0) = ' + CAST(@epoch_ticks AS NVARCHAR(40)) + N';
    DECLARE @epoch_offset NUMERIC(38,0) = ' + CAST(@cfg_offset AS NVARCHAR(40)) + N';
    DECLARE @precision_bits INT = ' + CAST(@precision_bits AS NVARCHAR(5)) + N';

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

    -- Calculate current time AFTER acquiring the lock
    DECLARE @now_ticks NUMERIC(38,0) = ' + @now_ticks_expr + N'
                                    - @epoch_ticks
                                    + @epoch_offset;

    -- Clock rollback detection (50000 ticks threshold)
    DECLARE @rollback_ticks NUMERIC(38,0) = @last_time - @now_ticks;
    IF @rollback_ticks > 0
    BEGIN
        IF @rollback_ticks < 50000
        BEGIN
            DECLARE @soft_msg NVARCHAR(200) = CONCAT(''clock rollback detected for ranj node '', @in_node_id, '' ('', CAST(@rollback_ticks AS NVARCHAR(40)), '' ticks)'');
            ROLLBACK TRANSACTION;
            THROW 50021, @soft_msg, 1;
        END
        ELSE
        BEGIN
            DECLARE @hard_msg NVARCHAR(200) = CONCAT(''hard clock rollback detected for ranj node '', @in_node_id, '' ('', CAST(@rollback_ticks AS NVARCHAR(40)), '' ticks)'');
            ROLLBACK TRANSACTION;
            THROW 50023, @hard_msg, 1;
        END
    END

    -- Determine starting tick and sequence
    DECLARE @current_tick NUMERIC(38,0) = CASE WHEN @now_ticks > @last_time THEN @now_ticks ELSE @last_time END;
    DECLARE @next_seq INT = CASE WHEN @current_tick = @last_time THEN @last_seq + 1 ELSE 0 END;

    -- Check capacity
    DECLARE @available_this_tick INT = 65536 - @next_seq;
    IF @allow_spanning = 0 AND @requested_count > @available_this_tick
    BEGIN
        DECLARE @cap_msg NVARCHAR(400) = CONCAT(
            ''requested '', @requested_count,
            '' IDs but only '', @available_this_tick,
            '' remain in tick '', CAST(@current_tick AS NVARCHAR(40)),
            '' for ranj node '', @in_node_id
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
    -- 89-bit timestamp: ts_high(48) | ts_mid(12) | ts_low(29)
    DECLARE @pow2_41 NUMERIC(38,0) = POWER(@two, 41);
    DECLARE @pow2_48 NUMERIC(38,0) = POWER(@two, 48);
    DECLARE @pow2_29 NUMERIC(38,0) = POWER(@two, 29);
    DECLARE @pow2_12 NUMERIC(38,0) = POWER(@two, 12);

    -- Variables used inside the loop
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

        -- Lower 8 bytes layout:
        --   bit 63:      variant bit 1 (set by 0x8000000000000000)
        --   bit 62:      variant bit 0 (0)
        --   bits 60-61:  precision (2 bits)
        --   bits 31-59:  ts_low (29 bits)
        --   bits 16-30:  node_id (15 bits)
        --   bits 0-15:   sequence (16 bits)
        SET @lo_base = CAST(0x8000000000000000 AS BIGINT)
                     | (CAST(@precision_bits AS BIGINT) * POWER(CAST(2 AS BIGINT), 60))
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
END';

    EXEC sp_executesql @sql;

    -- ----------------------------------------------------------------
    -- 7. Regenerate generate_ranjid (single-ID wrapper)
    -- ----------------------------------------------------------------
    SET @sql = N'
CREATE OR ALTER PROCEDURE generate_ranjid
    @in_node_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC generate_ranjids @in_node_id = @in_node_id, @requested_count = 1, @allow_spanning = 1;
END';

    EXEC sp_executesql @sql;

    -- ----------------------------------------------------------------
    -- 8. Reset node state (precision/epoch change invalidates stored timestamps)
    -- ----------------------------------------------------------------
    UPDATE heer_node_state
    SET last_id_time = 0, last_sequence = 0, updated_at = SYSUTCDATETIME();

    UPDATE heer_ranj_node_state
    SET last_id_time = 0, last_sequence = 0, updated_at = SYSUTCDATETIME();

    -- ----------------------------------------------------------------
    -- 9. Smoke test
    -- ----------------------------------------------------------------
    DECLARE @smoke_heerid TABLE (id BIGINT);
    DECLARE @smoke_ranjid TABLE (id BINARY(16));

    INSERT INTO @smoke_heerid
    EXEC generate_id @in_node_id = 1;

    INSERT INTO @smoke_ranjid
    EXEC generate_ranjid @in_node_id = 1;

    DECLARE @hid BIGINT;
    DECLARE @rid BINARY(16);
    SELECT @hid = id FROM @smoke_heerid;
    SELECT @rid = id FROM @smoke_ranjid;

    IF @hid IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50310, 'Smoke test failed: generate_id returned NULL', 1;
    END

    IF @rid IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50311, 'Smoke test failed: generate_ranjid returned NULL', 1;
    END

    COMMIT TRANSACTION;

    PRINT CONCAT('heer_configure succeeded. smoke HeerId=', @hid,
                 ', smoke RanjId=0x', CONVERT(NVARCHAR(40), @rid, 2));

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

-- Only explicit GRANT can run this
REVOKE EXECUTE ON heer_configure FROM PUBLIC;
GO
