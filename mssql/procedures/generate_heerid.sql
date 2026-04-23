CREATE OR ALTER PROCEDURE generate_ids
    @in_node_id      INT = NULL,
    @requested_count INT,
    @allow_spanning  BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    IF @requested_count IS NULL OR @requested_count <= 0
        THROW 50100, 'requested_count must be greater than zero', 1;

    -- Resolve node_id from session context if not provided
    IF @in_node_id IS NULL
    BEGIN
        SET @in_node_id = dbo.heer_current_node_id();
        IF @in_node_id IS NULL
            THROW 50101, 'heer_node_id is not set for this session', 1;
    END

    -- Validate node
    EXEC heer_set_node_id @in_node_id;

    -- Read epoch
    DECLARE @epoch_ms BIGINT;
    SELECT @epoch_ms = DATEDIFF_BIG(MILLISECOND, '1970-01-01T00:00:00', epoch)
    FROM heer_config
    WHERE id = 1;

    IF @epoch_ms IS NULL
        THROW 50102, 'heer_config row id=1 must exist before generating IDs', 1;

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

    -- Calculate current time AFTER acquiring the lock to avoid false clock rollback
    -- under concurrency (another thread may have advanced last_id_time while we waited)
    DECLARE @now_ms BIGINT = DATEDIFF_BIG(MILLISECOND, '1970-01-01T00:00:00', SYSUTCDATETIME()) - @epoch_ms;

    -- Clock rollback detection
    DECLARE @rollback_ms BIGINT = @last_time - @now_ms;
    IF @rollback_ms > 0
    BEGIN
        IF @rollback_ms < 2
        BEGIN
            DECLARE @drift_msg NVARCHAR(200) = CONCAT('logical future drift for node ', @in_node_id, ' (', @rollback_ms, ' ms) — likely batch-induced, check batch sizing');
            ROLLBACK TRANSACTION;
            THROW 50021, @drift_msg, 1;
        END
        ELSE IF @rollback_ms < 50
        BEGIN
            DECLARE @soft_msg NVARCHAR(200) = CONCAT('clock rollback detected for node ', @in_node_id, ' (', @rollback_ms, ' ms)');
            ROLLBACK TRANSACTION;
            THROW 50020, @soft_msg, 1;
        END
        ELSE
        BEGIN
            DECLARE @hard_msg NVARCHAR(200) = CONCAT('hard clock rollback detected for node ', @in_node_id, ' (', @rollback_ms, ' ms)');
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
            'requested ', @requested_count,
            ' IDs but only ', @available_this_tick,
            ' remain in millisecond ', @current_tick,
            ' for node ', @in_node_id
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
END
GO

CREATE OR ALTER PROCEDURE generate_id
    @in_node_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    EXEC generate_ids @in_node_id = @in_node_id, @requested_count = 1, @allow_spanning = 1;
END
GO
