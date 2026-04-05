IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'heer_nodes')
CREATE TABLE heer_nodes (
    node_id       INT PRIMARY KEY,
    name          NVARCHAR(255) NOT NULL,
    description   NVARCHAR(MAX),
    is_active     BIT DEFAULT 1,
    created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    last_accessed DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'heer_config')
CREATE TABLE heer_config (
    id                  INT PRIMARY KEY CHECK (id = 1),
    epoch               DATETIME2 NOT NULL,
    precision           VARCHAR(2) NOT NULL DEFAULT 'ns',
    ranj_epoch_offset   NUMERIC(38,0) NOT NULL DEFAULT 0,
    updated_at          DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'heer_node_state')
CREATE TABLE heer_node_state (
    node_id         INT PRIMARY KEY
                    REFERENCES heer_nodes(node_id) ON DELETE CASCADE,
    last_id_time    BIGINT NOT NULL DEFAULT 0,
    last_sequence   SMALLINT NOT NULL DEFAULT 0,
    updated_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'heer_ranj_node_state')
CREATE TABLE heer_ranj_node_state (
    node_id         INT PRIMARY KEY
                    REFERENCES heer_nodes(node_id) ON DELETE CASCADE,
    last_id_time    NUMERIC(38,0) NOT NULL DEFAULT 0,
    last_sequence   INT NOT NULL DEFAULT 0,
    updated_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
