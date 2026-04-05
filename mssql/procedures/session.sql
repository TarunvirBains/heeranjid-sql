CREATE OR ALTER PROCEDURE heer_set_node_id
    @node_id INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @node_id IS NULL
        THROW 50001, 'node_id cannot be null', 1;

    IF @node_id < 0 OR @node_id > 511
    BEGIN
        DECLARE @heer_range_msg NVARCHAR(200) = CONCAT('node_id ', @node_id, ' is out of range for HeerId');
        THROW 50002, @heer_range_msg, 1;
    END

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes
        WHERE node_id = @node_id AND is_active = 1
    )
    BEGIN
        DECLARE @heer_active_msg NVARCHAR(200) = CONCAT('node_id ', @node_id, ' is not registered as an active Heer node');
        THROW 50003, @heer_active_msg, 1;
    END

    EXEC sp_set_session_context N'heer_node_id', @node_id;
END
GO

CREATE OR ALTER PROCEDURE heer_set_ranj_node_id
    @node_id INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @node_id IS NULL
        THROW 50010, 'node_id cannot be null', 1;

    IF @node_id < 0 OR @node_id > 32767
    BEGIN
        DECLARE @ranj_range_msg NVARCHAR(200) = CONCAT('node_id ', @node_id, ' is out of range for RanjId');
        THROW 50011, @ranj_range_msg, 1;
    END

    IF NOT EXISTS (
        SELECT 1 FROM heer_nodes
        WHERE node_id = @node_id AND is_active = 1
    )
    BEGIN
        DECLARE @ranj_active_msg NVARCHAR(200) = CONCAT('node_id ', @node_id, ' is not registered as an active Heer node');
        THROW 50012, @ranj_active_msg, 1;
    END

    EXEC sp_set_session_context N'heer_ranj_node_id', @node_id;
END
GO

CREATE OR ALTER FUNCTION dbo.heer_current_node_id()
RETURNS INT
AS
BEGIN
    DECLARE @val SQL_VARIANT = SESSION_CONTEXT(N'heer_node_id');

    IF @val IS NULL
        RETURN NULL;

    RETURN CAST(@val AS INT);
END
GO

CREATE OR ALTER FUNCTION dbo.heer_current_ranj_node_id()
RETURNS INT
AS
BEGIN
    DECLARE @val SQL_VARIANT = SESSION_CONTEXT(N'heer_ranj_node_id');

    IF @val IS NULL
        RETURN NULL;

    RETURN CAST(@val AS INT);
END
GO
